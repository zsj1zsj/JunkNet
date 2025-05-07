import asyncio
import ray
import time
import random
import uuid
import logging
from collections import defaultdict, deque
import psutil # For actual hardware stats, if desired

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration ---
HEARTBEAT_INTERVAL = 10 # seconds for worker check-in (conceptual)
TASK_CHECK_INTERVAL = 2 # seconds for server to check task statuses
NODE_PERF_TEST_INTERVAL = 60 * 5 # seconds for nodes to "re-evaluate" capabilities (conceptual)
MAX_TASK_RETRIES = 3

# --- Helper Functions ---
def generate_node_id():
    return f"node_{uuid.uuid4().hex[:6]}"

def generate_task_id(prefix="task"):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

# --- Worker Node Actor ---
@ray.remote
class WorkerNode:
    def __init__(self, node_id=None):
        self.node_id = node_id if node_id else generate_node_id()
        self.capabilities = self._get_initial_capabilities()
        self.busy = False
        self.current_task_id = None
        logger.info(f"WorkerNode {self.node_id} initialized with capabilities: {self.capabilities}")

    def _get_initial_capabilities(self):
        # Simulate or use psutil for real hardware info
        # For simulation:
        cpu_power = random.uniform(0.5, 2.0) # Relative processing power
        memory_gb = random.randint(2, 16)    # Available memory in GB
        # Using psutil (example, can be more detailed):
        # cpu_power = psutil.cpu_count()
        # memory_gb = psutil.virtual_memory().available / (1024**3)
        return {
            "cpu_power": round(cpu_power, 2),
            "memory_gb": memory_gb,
            "last_updated": time.time()
        }

    def get_id(self):
        return self.node_id

    def get_capabilities(self):
        return self.capabilities

    def is_busy(self):
        return self.busy

    def run_performance_test(self):
        """Simulates running performance tests and updating capabilities."""
        logger.info(f"WorkerNode {self.node_id}: Running performance self-assessment.")
        # Simulate change or re-evaluate with psutil
        time.sleep(random.uniform(1, 3)) # Simulate test duration
        self.capabilities = self._get_initial_capabilities() # Re-assess
        logger.info(f"WorkerNode {self.node_id}: Capabilities updated: {self.capabilities}")
        return self.capabilities

    async def execute_sub_task(self, sub_task_id, task_data, complexity_factor=1):
        if self.busy:
            logger.warning(f"WorkerNode {self.node_id} received task {sub_task_id} but is already busy with {self.current_task_id}.")
            raise Exception(f"Worker {self.node_id} is busy.")

        self.busy = True
        self.current_task_id = sub_task_id
        logger.info(f"WorkerNode {self.node_id}: Starting sub_task {sub_task_id}. Data: {task_data}")

        try:
            # Simulate computation based on task complexity and node capability
            # A higher cpu_power means faster processing
            simulated_work_time = (task_data.get("base_compute_time", 2) * complexity_factor) / self.capabilities.get("cpu_power", 1)
            simulated_work_time = max(0.5, simulated_work_time) # Ensure some minimum time
            
            # Simulate memory requirement
            required_memory_gb = task_data.get("memory_needed_gb", 0.5)
            if required_memory_gb > self.capabilities.get("memory_gb", 0):
                logger.error(f"WorkerNode {self.node_id}: Not enough memory for {sub_task_id}. Required: {required_memory_gb}GB, Available: {self.capabilities.get('memory_gb')}GB")
                raise MemoryError(f"Not enough memory on {self.node_id} for {sub_task_id}")

            await asyncio.sleep(simulated_work_time) # Use asyncio.sleep for non-blocking wait in actor

            # Simulate a chance of failure for demonstration
            if random.random() < task_data.get("failure_probability", 0.05) and task_data.get("allow_failure_simulation", False):
                logger.warning(f"WorkerNode {self.node_id}: Simulating failure for sub_task {sub_task_id}")
                raise Exception(f"Simulated failure on {self.node_id} for {sub_task_id}")

            result = {"node_id": self.node_id, "sub_task_id": sub_task_id, "status": "success", "output": f"Processed data: {task_data.get('value', '')*2}"}
            logger.info(f"WorkerNode {self.node_id}: Completed sub_task {sub_task_id}. Result: {result['output']}")
            return result
        except Exception as e:
            logger.error(f"WorkerNode {self.node_id}: Error executing sub_task {sub_task_id}: {e}")
            return {"node_id": self.node_id, "sub_task_id": sub_task_id, "status": "failure", "error": str(e)}
        finally:
            self.busy = False
            self.current_task_id = None

# --- Central Server Actor ---
@ray.remote
class CentralServer:
    def __init__(self):
        self.workers = {} # node_id -> {"actor": ActorHandle, "capabilities": {}, "status": "idle", "last_heartbeat": time.time()}
        self.main_tasks = {} # main_task_id -> {"sub_tasks": {sub_task_id: status}, "results": [], "total_sub_tasks": N, "user_callback": None}
        
        # For managing sub_tasks and their assignments
        self.sub_task_queue = deque() # Stores (main_task_id, sub_task_id, sub_task_data, retries)
        self.active_sub_tasks = {} # sub_task_id -> {"main_task_id": str, "worker_id": str, "future": ray.ObjectRef, "start_time": float, "data": dict, "retries": int}
        self.sub_task_results = defaultdict(dict) # main_task_id -> {sub_task_id: result}
        
        self.scheduler_running = False
        logger.info("CentralServer initialized.")

    async def register_worker(self, worker_actor: WorkerNode):
        node_id = await worker_actor.get_id.remote()
        capabilities = await worker_actor.get_capabilities.remote()
        self.workers[node_id] = {
            "actor": worker_actor,
            "capabilities": capabilities,
            "status": "idle", # 'idle', 'busy', 'unresponsive'
            "last_heartbeat": time.time()
        }
        logger.info(f"CentralServer: Registered WorkerNode {node_id} with capabilities: {capabilities}")
        # Trigger scheduling if tasks are waiting
        if not self.scheduler_running and len(self.sub_task_queue) > 0:
            self._start_scheduler_if_needed()
        return f"Worker {node_id} registered successfully."

    def _get_available_workers(self):
        available = []
        for node_id, info in self.workers.items():
            if info["status"] == "idle": # Could also check last_heartbeat here
                # Check if node actor is alive (Ray will raise RayActorError if it's dead on next call)
                try:
                    # A lightweight call to check aliveness (or rely on Ray's built-in fault tolerance for actors)
                    # For this example, we'll assume Ray handles actor death detection when we try to assign a task.
                    available.append(node_id)
                except ray.exceptions.RayActorError:
                    logger.warning(f"CentralServer: Worker {node_id} seems to be dead. Marking as unresponsive.")
                    self.workers[node_id]["status"] = "unresponsive" # Or remove it
        return available

    def _select_worker_for_task(self, sub_task_data):
        available_workers = self._get_available_workers()
        if not available_workers:
            return None

        # Simple strategy: select based on CPU power (more sophisticated matching could be added)
        # Or just round-robin for simplicity in this example
        best_worker_id = None
        
        # Example: try to match memory requirements
        required_memory = sub_task_data.get("memory_needed_gb", 0)
        
        # Filter workers that meet memory requirements and are idle
        suitable_workers = []
        for node_id in available_workers:
            worker_info = self.workers[node_id]
            if worker_info["capabilities"].get("memory_gb", 0) >= required_memory:
                suitable_workers.append(node_id)
        
        if not suitable_workers:
            logger.warning(f"No suitable worker found for task needing {required_memory}GB RAM from available workers.")
            return None # No worker can handle memory requirement, or all are busy

        # Simple round-robin among suitable workers or pick one with most CPU
        # For now, just pick the first suitable one
        best_worker_id = random.choice(suitable_workers) # Or a more deterministic choice

        # logger.info(f"Selected worker {best_worker_id} for task.")
        return best_worker_id


    async def submit_main_task(self, task_description: str, sub_task_definitions: list):
        """
        Submits a main task composed of several sub-tasks.
        sub_task_definitions: list of dicts, each dict is sub_task_data 
                              (e.g., {"value": i, "base_compute_time": 2, "complexity_factor": 1.5, "memory_needed_gb": 1})
        """
        main_task_id = generate_task_id("main")
        logger.info(f"CentralServer: Received main task {main_task_id} ('{task_description}') with {len(sub_task_definitions)} sub-tasks.")
        
        self.main_tasks[main_task_id] = {
            "description": task_description,
            "sub_tasks_status": {}, # sub_task_id -> 'pending' | 'running' | 'completed' | 'failed'
            "total_sub_tasks": len(sub_task_definitions),
            "expected_sub_tasks_ids": set()
        }

        for i, sub_task_data_template in enumerate(sub_task_definitions):
            sub_task_id = generate_task_id(f"{main_task_id}_sub{i}")
            self.main_tasks[main_task_id]["sub_tasks_status"][sub_task_id] = "pending"
            self.main_tasks[main_task_id]["expected_sub_tasks_ids"].add(sub_task_id)
            
            # Augment sub_task_data with IDs
            sub_task_data = sub_task_data_template.copy()
            sub_task_data["_main_task_id"] = main_task_id # internal tracking
            sub_task_data["_sub_task_id"] = sub_task_id   # internal tracking

            self.sub_task_queue.append((main_task_id, sub_task_id, sub_task_data, 0)) # main_id, sub_id, data, retries

        self._start_scheduler_if_needed()
        return main_task_id

    def _start_scheduler_if_needed(self):
        if not self.scheduler_running:
            self.scheduler_running = True
            logger.info("CentralServer: Creating asyncio task for _scheduler_loop.")
            # self._scheduler_loop() 是这个 actor 实例的一个 async 方法。
            # asyncio.create_task 会在 actor 的事件循环上安排它。
            asyncio.create_task(self._scheduler_loop())

    async def _scheduler_loop(self):
        """Continuously tries to schedule tasks and monitor active tasks."""
        logger.info("CentralServer: Scheduler loop started.")
        while self.scheduler_running:
            # 1. Try to dispatch new tasks from the queue
            if self.sub_task_queue:
                worker_id = self._select_worker_for_task(self.sub_task_queue[0][2]) # peek at first task's data
                if worker_id:
                    main_task_id, sub_task_id, sub_task_data, retries = self.sub_task_queue.popleft()
                    worker_actor = self.workers[worker_id]["actor"]
                    self.workers[worker_id]["status"] = "busy"
                    
                    logger.info(f"CentralServer: Assigning sub_task {sub_task_id} (main: {main_task_id}, retries: {retries}) to Worker {worker_id}.")
                    
                    # Add complexity based on retry if needed, or task data might already have it
                    complexity = sub_task_data.get("complexity_factor", 1)

                    future = worker_actor.execute_sub_task.remote(sub_task_id, sub_task_data, complexity)
                    self.active_sub_tasks[sub_task_id] = {
                        "main_task_id": main_task_id,
                        "worker_id": worker_id,
                        "future": future,
                        "start_time": time.time(),
                        "data": sub_task_data,
                        "retries": retries
                    }
                    self.main_tasks[main_task_id]["sub_tasks_status"][sub_task_id] = "running"

            # 2. Check status of active tasks (non-blocking)
            if self.active_sub_tasks:
                futures_to_check = {sub_id: task_info["future"] for sub_id, task_info in self.active_sub_tasks.items()}
                # Check for any completed/failed tasks without blocking indefinitely
                # timeout=0 means non-blocking check
                ready_futures, remaining_futures = ray.wait(list(futures_to_check.values()), num_returns=len(futures_to_check), timeout=0)

                for future in ready_futures:
                    # Find which sub_task this future belongs to
                    done_sub_task_id = None
                    for sub_id, task_info in self.active_sub_tasks.items():
                        if task_info["future"] == future:
                            done_sub_task_id = sub_id
                            break
                    
                    if done_sub_task_id:
                        task_info = self.active_sub_tasks.pop(done_sub_task_id) # Remove from active
                        main_task_id = task_info["main_task_id"]
                        worker_id = task_info["worker_id"]
                        sub_task_data = task_info["data"]
                        retries = task_info["retries"]

                        try:
                            result = await future # Get the actual result or exception
                            if result["status"] == "success":
                                logger.info(f"CentralServer: Sub_task {done_sub_task_id} completed successfully by {worker_id}.")
                                self.sub_task_results[main_task_id][done_sub_task_id] = result
                                self.main_tasks[main_task_id]["sub_tasks_status"][done_sub_task_id] = "completed"
                                if worker_id in self.workers: self.workers[worker_id]["status"] = "idle"
                            else: # Task reported failure
                                logger.warning(f"CentralServer: Sub_task {done_sub_task_id} failed on {worker_id}. Error: {result.get('error')}")
                                self._handle_failed_sub_task(main_task_id, done_sub_task_id, sub_task_data, retries, worker_id, reason=result.get('error'))

                        except ray.exceptions.RayActorError:
                            logger.error(f"CentralServer: Worker {worker_id} died or became unreachable while executing {done_sub_task_id}.")
                            # Mark worker as unresponsive or remove
                            if worker_id in self.workers: self.workers[worker_id]["status"] = "unresponsive" 
                            self._handle_failed_sub_task(main_task_id, done_sub_task_id, sub_task_data, retries, worker_id, reason="Worker actor error")
                        except Exception as e: # Other exceptions during ray.get() or from the task itself if not caught by worker
                            logger.error(f"CentralServer: Exception getting result for {done_sub_task_id} from {worker_id}: {e}")
                            self._handle_failed_sub_task(main_task_id, done_sub_task_id, sub_task_data, retries, worker_id, reason=str(e))
            
            # 3. Check for stalled tasks (tasks that have been running too long) - Optional
            # ... (implementation for task timeouts) ...

            # 4. Check if any main task is fully completed
            for mt_id, mt_info in list(self.main_tasks.items()): # list() for safe iteration if modifying
                if mt_id in self.sub_task_results and len(self.sub_task_results[mt_id]) == mt_info["total_sub_tasks"]:
                    # Check if all sub_tasks are 'completed' (not just present in results, in case some failed permanently)
                    all_completed = True
                    for sub_id in mt_info["expected_sub_tasks_ids"]:
                        if mt_info["sub_tasks_status"].get(sub_id) != "completed":
                            all_completed = False
                            break
                    if all_completed:
                        logger.info(f"CentralServer: Main task {mt_id} fully completed!")
                        # Potentially call a user callback or mark as ready for collection
                        # For simplicity, we just log. Results are in self.sub_task_results[mt_id]

            # If no active tasks and no queue, and scheduler is running, maybe stop? Or just keep polling.
            if not self.active_sub_tasks and not self.sub_task_queue:
                 # Optionally, if you want the scheduler to stop if idle for a while
                 # logger.info("Scheduler idle. Will continue polling.")
                 pass


            await asyncio.sleep(TASK_CHECK_INTERVAL) # Interval for the scheduler loop

        logger.info("CentralServer: Scheduler loop stopped.")
        self.scheduler_running = False


    def _handle_failed_sub_task(self, main_task_id, sub_task_id, sub_task_data, retries_done, failed_worker_id, reason="Unknown"):
        if failed_worker_id in self.workers and self.workers[failed_worker_id]["status"] != "unresponsive":
            self.workers[failed_worker_id]["status"] = "idle" # Mark worker idle unless it's completely unresponsive

        if retries_done < MAX_TASK_RETRIES:
            logger.info(f"CentralServer: Re-queuing sub_task {sub_task_id} (main: {main_task_id}). Retries left: {MAX_TASK_RETRIES - retries_done -1}.")
            self.sub_task_queue.append((main_task_id, sub_task_id, sub_task_data, retries_done + 1))
            self.main_tasks[main_task_id]["sub_tasks_status"][sub_task_id] = "pending_retry"
        else:
            logger.error(f"CentralServer: Sub_task {sub_task_id} (main: {main_task_id}) failed permanently after {MAX_TASK_RETRIES} retries. Last error: {reason}")
            self.main_tasks[main_task_id]["sub_tasks_status"][sub_task_id] = "failed_permanently"
            # Store a failure marker in results so get_main_task_results knows it's terminally failed
            self.sub_task_results[main_task_id][sub_task_id] = {"status": "failed_permanently", "error": reason, "original_task_data": sub_task_data}
            # If a single sub_task fails permanently, the main task might be considered failed or partially completed.

    async def get_main_task_status(self, main_task_id: str):
        if main_task_id not in self.main_tasks:
            return {"error": "Main task ID not found."}
        
        info = self.main_tasks[main_task_id]
        completed_count = 0
        failed_permanently_count = 0
        
        for sub_id in info["expected_sub_tasks_ids"]:
            status = info["sub_tasks_status"].get(sub_id)
            if status == "completed":
                completed_count +=1
            elif status == "failed_permanently":
                failed_permanently_count +=1
        
        total_processed_subtasks = completed_count + failed_permanently_count
        
        status_summary = {
            "main_task_id": main_task_id,
            "description": info["description"],
            "total_sub_tasks": info["total_sub_tasks"],
            "completed_sub_tasks": completed_count,
            "failed_sub_tasks": failed_permanently_count,
            "pending_or_running": info["total_sub_tasks"] - total_processed_subtasks,
            "overall_status": "unknown"
        }

        if total_processed_subtasks == info["total_sub_tasks"]:
            if failed_permanently_count > 0:
                status_summary["overall_status"] = "completed_with_failures" if completed_count > 0 else "failed"
            else:
                status_summary["overall_status"] = "completed_successfully"
        elif total_processed_subtasks < info["total_sub_tasks"]:
             status_summary["overall_status"] = "in_progress"
        
        return status_summary

    async def get_main_task_results(self, main_task_id: str):
        status = await self.get_main_task_status(main_task_id)
        if status.get("overall_status") in ["completed_successfully", "completed_with_failures", "failed"]:
            return {
                "status_summary": status,
                "sub_task_details": self.sub_task_results.get(main_task_id, {})
            }
        return {"status_summary": status, "message": "Task still in progress or not found."}

    async def update_worker_capabilities(self, node_id: str, capabilities: dict):
        if node_id in self.workers:
            self.workers[node_id]["capabilities"] = capabilities
            self.workers[node_id]["last_updated"] = time.time()
            logger.info(f"CentralServer: Updated capabilities for Worker {node_id}: {capabilities}")
        else:
            logger.warning(f"CentralServer: Attempted to update capabilities for unknown worker {node_id}")
            
    async def get_cluster_status(self):
        worker_info_list = []
        for node_id, data in self.workers.items():
            worker_info_list.append({
                "id": node_id,
                "status": data["status"],
                "capabilities": data["capabilities"],
                "last_heartbeat": data["last_heartbeat"] # You'd need a real heartbeat mechanism for this
            })
        return {
            "total_workers": len(self.workers),
            "task_queue_size": len(self.sub_task_queue),
            "active_sub_tasks_count": len(self.active_sub_tasks),
            "workers": worker_info_list
        }
    async def set_worker_status_debug(self, worker_id: str, status: str):
        """
        Sets the status of a worker. Intended for testing/debugging.
        """
        if worker_id in self.workers:
            self.workers[worker_id]["status"] = status
            logger.info(f"CentralServer: Debug - Set status of worker {worker_id} to '{status}'.")
            return True
        else:
            logger.warning(f"CentralServer: Debug - Attempted to set status for unknown worker {worker_id}.")
            return False
    


# --- Main Orchestration (Example Usage) ---
async def main_orchestration():
    # Initialize Ray (connects to an existing cluster or starts one locally)
    # Using a named actor for CentralServer allows other components to find it if needed.
    # namespace ensures actor names are unique if you run multiple instances of this script for testing
    # on the same Ray cluster.
    try:
        # For this example, we'll use a local Ray cluster.
        # If running on a real cluster, ray.init(address="auto") or specific address.
        if ray.is_initialized():
            ray.shutdown()
        ray.init(ignore_reinit_error=True, namespace="distributed_computing_project") 
        logger.info("Ray initialized.")

        # 1. Create Central Server
        # Name the actor so it can be found (e.g., for the scheduler loop to call itself)
        server = CentralServer.options(name="CentralServer", get_if_exists=True).remote()
        logger.info("CentralServer actor created/retrieved.")

        # 2. Create and Register Worker Nodes
        num_workers = 3
        worker_nodes = [WorkerNode.remote(node_id=f"laptop_{i}") for i in range(num_workers)]
        logger.info(f"Created {num_workers} WorkerNode actors.")

        registration_tasks = [server.register_worker.remote(worker) for worker in worker_nodes]
        registration_results = await asyncio.gather(*registration_tasks)
        for res in registration_results:
            logger.info(f"Registration result: {res}")
        
        await asyncio.sleep(1) # give time for registration and scheduler to potentially start

        # 3. Define and Submit a Main Task
        # Task: Calculate squares of numbers, simulate some requiring more memory/cpu
        main_task_desc = "Calculate squares and simulate resource needs"
        sub_tasks_definitions = []
        cluster_status_before_submit = await server.get_cluster_status.remote()
        worker_capabilities_list = [w_info['capabilities'] for w_info in cluster_status_before_submit.get('workers', []) if w_info.get('capabilities')]
        random_worker_caps = random.choice(worker_capabilities_list)
        max_worker_memory_gb = random_worker_caps.get('memory_gb', 4.0)
        for i in range(10): # 10 sub-tasks
            complexity = random.uniform(0.8, 2.5)
            memory_need = random.uniform(0.2, max_worker_memory_gb * 0.8)
            #memory_need = random.uniform(0.2, 4.0) # GB
            sub_tasks_definitions.append({
                "value": i, 
                "base_compute_time": 1.5, # Base seconds for computation
                "complexity_factor": round(complexity,1), # Multiplier for compute time
                "memory_needed_gb": round(memory_need,1),
                "allow_failure_simulation": True, # Enable simulated failures for some tasks
                "failure_probability": 0.1 # 10% chance this sub-task might fail on a worker
            })
        
        main_task_id = await server.submit_main_task.remote(main_task_desc, sub_tasks_definitions)
        logger.info(f"Submitted main task {main_task_id} to CentralServer.")

        # 4. Monitor Task Progress and Get Results
        max_wait_time = 120  # seconds
        start_poll_time = time.time()
        final_results = None

        while time.time() - start_poll_time < max_wait_time:
            status_summary = await server.get_main_task_status.remote(main_task_id)
            logger.info(f"Polling Task Status ({main_task_id}): {status_summary}")

            if status_summary.get("overall_status") in ["completed_successfully", "completed_with_failures", "failed"]:
                logger.info(f"Main task {main_task_id} reached terminal state: {status_summary.get('overall_status')}")
                final_results = await server.get_main_task_results.remote(main_task_id)
                break
            
            cluster_stat = await server.get_cluster_status.remote()
            logger.debug(f"Cluster Status: {cluster_stat['total_workers']} workers, {cluster_stat['task_queue_size']} queued, {cluster_stat['active_sub_tasks_count']} active.")
            
            await asyncio.sleep(5) # Poll every 5 seconds
        else: # Loop timed out
            logger.warning(f"Main task {main_task_id} did not complete within {max_wait_time}s. Fetching current results.")
            final_results = await server.get_main_task_results.remote(main_task_id)

        if final_results:
            logger.info(f"\n--- FINAL RESULTS for {main_task_id} ---")
            logger.info(f"Status Summary: {final_results.get('status_summary')}")
            # logger.info("Sub-task Details:")
            # for sub_id, detail in final_results.get('sub_task_details', {}).items():
            #     logger.info(f"  {sub_id}: Status: {detail.get('status')}, Output: {detail.get('output', {}).get('output', 'N/A' if detail.get('status')=='success' else detail.get('error'))}")
            # logger.info("--- END OF RESULTS ---")
        else:
            logger.error(f"Could not retrieve final results for {main_task_id}.")

        # 5. Simulate a worker updating its capabilities (e.g., after idle performance test)
        # This would typically be initiated by the worker itself periodically.
        if worker_nodes:
            target_worker_actor = worker_nodes[0]
            node_id_to_update = await target_worker_actor.get_id.remote()
            logger.info(f"Simulating Worker {node_id_to_update} running performance test...")
            new_caps = await target_worker_actor.run_performance_test.remote()
            await server.update_worker_capabilities.remote(node_id_to_update, new_caps)
            logger.info(f"Requested capability update for {node_id_to_update} on server.")
            await asyncio.sleep(1)
            cluster_stat_after_update = await server.get_cluster_status.remote()
            logger.info(f"Cluster status after capability update: {cluster_stat_after_update['workers']}")


        # 6. Simulate adding a new node dynamically (Scalability)
        logger.info("\n--- Simulating adding a new node ---")
        new_node = WorkerNode.remote(node_id="desktop_new")
        await server.register_worker.remote(new_node)
        logger.info(f"New node {await new_node.get_id.remote()} registered.")
        await asyncio.sleep(1)
        cluster_stat_after_add = await server.get_cluster_status.remote()
        logger.info(f"Cluster status after adding node: {cluster_stat_after_add['workers']}")


        # 7. Simulate a worker node failing (Fault Tolerance)
        # For this, we would need to "kill" an actor or have it raise an unhandled exception.
        # Ray handles actor death by raising RayActorError on the caller side.
        # The CentralServer's _scheduler_loop already has try-except for RayActorError.
        if worker_nodes: # Ensure there are workers to "kill"
            logger.info("\n--- Simulating a worker failure ---")
            failing_worker_actor = worker_nodes[0] # Choose the first worker
            failing_worker_id = await failing_worker_actor.get_id.remote()
            logger.info(f"Attempting to 'kill' worker: {failing_worker_id}. Subsequent tasks for it should fail over.")
            
            # To simulate, we'll submit a new small task that would ideally go to this worker
            # and then kill the worker.
            # First, ensure the worker is idle to make it a candidate.
            # if failing_worker_id in server.workers.items(): # Check if server knows this worker
            #      server.workers[failing_worker_id]["status"] = "idle" 
            # Set the worker's status to 'idle' on the server using the new method
            logger.info(f"Setting worker {failing_worker_id} status to 'idle' on server before submitting failure test task.")
            set_success = await server.set_worker_status_debug.remote(failing_worker_id, "idle")
            if not set_success:
                logger.warning(f"Could not set worker {failing_worker_id} to idle. Test may not behave as expected.")

                 
            # Submit a task that should ideally be picked up by the 'failing_worker' if it's the only idle one
            # Or just make it a high priority task if we had such logic
            dummy_failure_test_task_def = [{"value": "failure_test", "base_compute_time": 0.1, "complexity_factor": 1, "memory_needed_gb": 0.1}]
            failure_test_main_task_id = await server.submit_main_task.remote("Failure Test Task", dummy_failure_test_task_def)

            await asyncio.sleep(2) # Give scheduler time to assign it. Check active_sub_tasks on server.
            
            # Find the sub_task and its assigned worker
            # active_tasks_info = await server.active_sub_tasks 
            # This is not directly accessible as it's internal state.
            # We'd need a method on server to get this for test purposes
            # Or, rely on observing logs.
            
            # Simulate killing the actor
            try:
                ray.kill(failing_worker_actor)
                logger.info(f"Killed worker actor {failing_worker_id}. The CentralServer should detect this on next interaction or task completion check.")
            except Exception as e:
                logger.error(f"Could not kill worker {failing_worker_id}: {e} (May already be dead or not killable this way in all Ray versions/setups easily from script).")
            
            # Wait for the system to react and potentially re-schedule
            logger.info("Waiting for system to handle worker failure and re-schedule tasks...")
            await asyncio.sleep(TASK_CHECK_INTERVAL * 3) # Wait a few check cycles

            failure_task_status = await server.get_main_task_status.remote(failure_test_main_task_id)
            logger.info(f"Status of failure test task after worker kill: {failure_task_status}")
            if failure_task_status.get("overall_status") == "completed_successfully":
                logger.info("Fault tolerance test: Task was successfully rescheduled and completed after worker failure.")
            else:
                logger.warning("Fault tolerance test: Task may not have been rescheduled successfully or is still pending.")


    except Exception as e:
        logger.exception(f"An error occurred during orchestration: {e}")
    finally:
        logger.info("Shutting down Ray...")
        ray.shutdown()
        logger.info("Ray shut down.")

if __name__ == "__main__":
    import asyncio
    # Python 3.7+ for asyncio.run
    asyncio.run(main_orchestration())
