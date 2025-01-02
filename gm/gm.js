const express = require('express');
const fs = require('fs');
const axios = require('axios');
const winston = require('winston');
const path = require('path');


class JobRegister {
    constructor() {
        if (JobRegister.instance) {
            return JobRegister.instance;  // Return existing instance
        }

        this.jobs = {};  // Store jobs by their jobId
        JobRegister.instance = this;  // Assign the singleton instance
    }

    registerJob(job) {
        this.jobs[job.jobId] = job;
    }

    getJobById(jobId) {
        return this.jobs[jobId];
    }

    removeJob(jobId) {
        delete this.jobs[jobId];
    }
}



class Task {
    constructor(taskId, job, duration) {
        this.taskId = taskId;
        this.startTime = job.startTime;
        this.scheduledTime = null;
        this.endTime = null;
        // this.job = job;
        this.jobId = job.jobId;
        this.duration = duration;
        this.nodeId = null;
        this.schedulingAttempts = 0;
        this.constraints = [];
        this.communicationDelay = 0;
        this.repartitions = 0;
        this.inconsistencies = 0;
        this.partitionId = null;
        this.gmId = null;
        this.lm = null;
        this.gmNodeAddress = null;
    }
}

class Job {
    static jobCount = 1;
    static jobStartTimestamps = {};

    constructor(startTime, numTasks, taskDurations, jobId, jobType, maxExecutionTime) {
        this.startTime = this._adjustStartTime(startTime);
        this.numTasks = numTasks;
        this.tasks = {};
        this.taskCounter = 0;
        this.completedTasks = [];
        this.completionTime = -1.0;
        this.isShort = Math.max(...taskDurations) < 90.5811;
        this.vals = taskDurations;
        this.jobType = jobType;
        this.jobId = jobId;
        this.idealCompletionTime = maxExecutionTime;
        const jobRegister = new JobRegister();  // Access singleton instance


        this._createTasks(taskDurations);
        this.sortedTaskIds = Object.keys(this.tasks).sort((a, b) =>
            this.tasks[a].duration - this.tasks[b].duration);
        jobRegister.registerJob(this);  // Register the job in the global register
    }

    _adjustStartTime(startTime) {
        if (!Job.jobStartTimestamps[startTime]) {
            Job.jobStartTimestamps[startTime] = startTime;
        } else {
            Job.jobStartTimestamps[startTime] += 0.01;
            startTime = Job.jobStartTimestamps[startTime];
        }
        return startTime;
    }

    _createTasks(taskDurations) {
        taskDurations.forEach(duration => {
            this.taskCounter++;
            const taskId = String(this.taskCounter);
            this.tasks[taskId] = new Task(taskId, this, duration);
        });
    }
}

class DataCenterManager {
    constructor(configPath = 'datacenter_config.json') {
        this.app = express();
        this.app.use(express.json());
        this.config = this._loadDatacenterConfig(configPath);
        // this.gmId = 1;

        const podName = process.env.GM_ID; // e.g., "lm-0"
        if (!podName) {
            throw new Error("GM_ID environment variable is not set. Ensure Kubernetes configuration is correct.");
        }

        // Parse the ordinal index from the pod name to set LM_ID
        const lmIdMatch = podName.match(/gm-(\d+)/);
        if (lmIdMatch) {
            this.gmId = parseInt(lmIdMatch[1], 10) + 1; // Convert "lm-0" to LM ID 1, "lm-1" to LM ID 2, etc.
        } else {
            throw new Error(`Invalid pod name format: ${podName}`);
        }

        console.log(`Logical Master (LM) ID set to: ${this.gmId}`);


        this.internalAvailableNodes = {};
        this.externalAvailableNodes = {};
        this.lmsList = Object.keys(this.config.LMs);
        this.gmsList = Object.keys(this.config.GMs);
        this.taskQueue = [];
        this.jobs = {};
        this.networkDelay = 0.1;
        this.completedJobs = [];
        //this.jobs = {}; // Optional, you can now rely on JobRegister for job management
        this.jobRegister = new JobRegister();  // No need to create a new instance anymore

        this._categorizeWorkerNodes();
        this._setupLogger();
        this._setupRoutes();
    }

    _setupLogger() {
        this.jobLogger = winston.createLogger({
            level: 'info',
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.printf(({ timestamp, message }) =>
                    `${timestamp} - ${message}`)
            ),
            transports: [
                new winston.transports.File({ filename: 'job_details.log' })
            ]
        });
    }

    _loadDatacenterConfig(configPath) {
        return JSON.parse(fs.readFileSync(configPath, 'utf8'));
    }

    _categorizeWorkerNodes() {
        this.lmsList.forEach(lmId => {
            // Initialize internal and external nodes for the current LM
            this.internalAvailableNodes[lmId] = [];
            this.externalAvailableNodes[lmId] = {};

            // Iterate through each partition in the LM
            Object.entries(this.config.LMs[lmId].partitions).forEach(([partitionId, partition]) => {
                // Check if worker_nodes exists and is an array
                if (Array.isArray(partition.worker_nodes)) {
                    if (partition.gm_id === this.gmId) {
                        // Add worker nodes to internal if they belong to this GM
                        this.internalAvailableNodes[lmId].push(...partition.worker_nodes);
                    } else {
                        // Initialize external nodes for the GM if not already done
                        if (!this.externalAvailableNodes[lmId][partition.gm_id]) {
                            this.externalAvailableNodes[lmId][partition.gm_id] = [];
                        }
                        // Add worker nodes to the corresponding external GM
                        this.externalAvailableNodes[lmId][partition.gm_id].push(...partition.worker_nodes);
                    }
                } else {
                    console.warn(`Warning: worker_nodes is not an array for partition ${partitionId} in LM ${lmId}`);
                }
            });

           
        });
        // Print internal and external available nodes
        // console.log(`Internal worker nodes for GM ${this.gmId} `, this.internalAvailableNodes);
        // console.log(`External worker nodes for GM ${this.gmId} `, this.externalAvailableNodes);
    }

    _setupRoutes() {
        this.app.post('/assign_job', this._handleAssignJob.bind(this));
        this.app.post('/task_complete', this._handleTaskComplete.bind(this));
        this.app.post('/update_gm', this._handleUpdateGM.bind(this));
        this.app.post('/unschedule_the_task', this._handleUnscheduleTask.bind(this));
    }

    _handleAssignJob(req, res) {

        const { job } = req.body;
        // console.log(job);


        if (!job) {
            return res.status(400).json({ status: "error", message: "Job object not provided" });
        }

        // Parse the received job object
        const parsedJob = new Job(
            Math.floor(Date.now()/1000),
            job.numTasks,
            job.executionTimes,
            job.jobId,
            job.jobType,
            job.maxExecutionTime
        );



        this.jobs[parsedJob.jobId] = parsedJob;

        // Schedule the job
        const response = this.scheduleJobBatched(parsedJob);

        return res.json(response);
    }


    _handleTaskComplete(req, res) {
        const { task } = req.body;
        const response = this.receiveTaskResponse(task);
        res.status(200).json(response);
    }

    _handleUpdateGM(req, res) {
        const { status } = req.body;
        const response = this.updateStatus(status);
        res.json(response);
    }

    _handleUnscheduleTask(req, res) {
        const { task } = req.body;
        const response = this.unscheduleTask(task);
        res.json(response);
    }

    _logJobCompletion(job) {
        const logMessage = `Job ID: ${job.jobId}, ` +
            `Start Time: ${job.startTime}, ` +
            `Completion Time: ${job.completionTime}, ` +
            `Delay: ${job.completionTime - job.startTime - job.idealCompletionTime}s`;
        this.jobLogger.info(logMessage);
    }

    scheduleJobBatched(job) {
        // console.log("inside schdeule jog:",job);

        job.gmId = this.gmId
        const jobId = job.jobId;

        console.log(this.gmId," ",jobId,"received");
        
        this.jobs[jobId] = job;
        let taskMappingRequestBatch = [];
        let prevLm = null;
        let noResources = false;

        for (const taskId in this.jobs[jobId].tasks) {
            // console.log("inside lopp", taskId)
            const task = this.jobs[jobId].tasks[taskId];

            task.gmId = this.gmId;
            task.gmNodeAddress = this.config.GMs[this.gmId].address;

            if (noResources) {
                this.taskQueue.unshift(task);
                continue;
            }
            // console.log("entering serach for nodes");

            let taskMappingRequest = this.scheduleTask(task);
            // console.log("entering serach for inernodes nodes copm");
            if (!taskMappingRequest) {
                taskMappingRequest = this.batchedRepartition(task);
                // console.log("entering serach for ext nodes copm");
                if (!taskMappingRequest) {
                    noResources = true;
                    if (taskMappingRequestBatch.length > 0) {
                        this.sendTaskBatch(taskMappingRequestBatch, prevLm);
                        // this.taskQueue.push(task);
                        // console.log(this.taskQueue);

                    }
                    taskMappingRequestBatch = [];
                    this.taskQueue.unshift(task);
                    continue;
                }
            }

            const currentLm = taskMappingRequest.lmId;
            if (prevLm === currentLm) {
                taskMappingRequestBatch.push(taskMappingRequest);
            } else if (prevLm === null) {
                prevLm = currentLm;
                taskMappingRequestBatch.push(taskMappingRequest);
            } else {
                // console.log("sending to schedule batcj");

                this.sendTaskBatch(taskMappingRequestBatch, prevLm);
                taskMappingRequestBatch = [];
                prevLm = currentLm;
                taskMappingRequestBatch.push(taskMappingRequest);
            }

            if (taskMappingRequestBatch.length === 100) {
                // console.log("sending to schedule batcj");

                this.sendTaskBatch(taskMappingRequestBatch, prevLm);
                prevLm = null;
                taskMappingRequestBatch = [];
            }
            // else {
            //     // Task has dependencies, add to task queue
            //     this.taskQueue.push(task);
            //     continue;
            // }
        }

        if (taskMappingRequestBatch.length > 0 && !noResources) {
            this.sendTaskBatch(taskMappingRequestBatch, prevLm);
            taskMappingRequestBatch = [];
        }

        return { status: noResources ? "failed" : "success" };
    }

    scheduleTask(task) {
        for (const lmId of this.lmsList) {
            if (this.internalAvailableNodes[lmId].length > 0) {
                // console.log("internal length", this.internalAvailableNodes[lmId]);

                const nodeAddress = this.internalAvailableNodes[lmId].shift();

                // Assign lmId, gmId, and nodeAddress to the task object
                task.lm = lmId;
                task.nodeId = nodeAddress;
                task.partitionId = this.gmId;

                return { task: task, lmId: lmId };
            }
        }
        return null; // No available nodes
    }

    batchedRepartition(task) {
        for (const lmId of this.lmsList) {
            for (const gmId of this.gmsList) {
                if (gmId === this.gmId) continue;

                if (this.externalAvailableNodes[lmId]?.[gmId]?.length > 0) {
                    // console.log("external length", this.externalAvailableNodes[lmId][gmId]);
                    const nodeAddress = this.externalAvailableNodes[lmId][gmId].shift();
                    task.repartitions++;

                    // Assign lmId, gmId, and nodeAddress to the task object
                    task.lm = lmId;
                    task.nodeId = nodeAddress;
                    task.partitionId = gmId;

                    return { task: task, lmId: lmId, externalPartition: gmId };
                }
            }
        }
        return null; // No available external nodes
    }

    async sendTaskBatch(taskBatch, lmId) {
        // console.log("inside schedule:", taskBatch);


        const lmAddress = this.config.LMs[lmId].address;
        try {
            // console.log("lm id", lmId);
            await axios.post(`http://${lmAddress}/process_tasks`, {
                task_batch: taskBatch
            });
        } catch (error) {
            console.error(`Failed to send tasks to LM ${lmId}`, error);
            this.taskQueue.unshift(taskBatch);
        }
    }

    async assignTaskToNode(task) {
        if (task) {
            const lmId = task.lm;
            const lmAddress = this.config.LMs[lmId].address;
            try {
                await axios.post(`http://${lmAddress}/assign_tasks_waiting`, {
                    task: task
                });
                return true;
            } catch (error) {
                console.error(`Failed to assign task to LM ${lmId}`, error);
            }
        }
        return false;
    }

    receiveTaskResponse(task) {
        const job = this.jobRegister.getJobById(task.jobId); // Get job from the register
        if (!job) {
            console.error(`Job ${task.jobId} not found.`);
            return { status: "error", message: "Job not found" };
        }

        const jobTask = job.tasks[task.taskId];
        if (!jobTask) {
            console.error(`Task ${task.taskId} in job ${task.jobId} not found.`);
            return { status: "error", message: "Task not found" };
        }

        if (job.completedTasks.some(t => t.taskId === task.taskId)) {
            console.error(`Duplicate task completion detected for task ${task.taskId} in job ${task.jobId}.`);
            return { status: "error", message: "Duplicate completion" };
        }

        // Mark task as completed
        job.completedTasks.push(jobTask);
        console.log(`Task completion received - Job ${job.jobId} / Task ${task.taskId} by GM ${this.gmId}`);
        // Check if all tasks for the job are complete
        if (Object.keys(job.tasks).length === job.completedTasks.length) {
            job.completionTime = Date.now()/1000;
            job.endTime = job.completionTime;
            const jobDelay = job.completionTime - job.startTime - job.idealCompletionTime;
            console.log(`Job Complete - Job ${job.jobId}, Delay: ${jobDelay}`);
            this.completedJobs.push(job);
            this._logJobCompletion(job);
        }


        // Check if there are pending tasks in the queue
        if (this.taskQueue.length > 0) {
            const newTask = this.taskQueue.shift();
            newTask.nodeId = task.nodeId;
            // console.log("new task nodeid:", newTask.nodeId);
            newTask.lm = task.lm;
            newTask.gmId = this.gmId;
            newTask.partitionId = task.partitionId;
            this.assignTaskToNode(newTask)
                .then(nodeAssigned => {
                    if (nodeAssigned) {
                        // console.log(`Assigned new task ${newTask.taskId} to node and task is cpmleted`);
                    } else {
                        console.log(`No available node for new task ${newTask.taskId}, re-adding to task queue.`);
                        this.taskQueue.unshift(newTask);
                    }
                });


        }
        if (this.taskQueue.length === 0) {
            if (task.partitionId === this.gmId) {
                // Add the node to internal_available_nodes for the LM
                this.internalAvailableNodes[task.lm].push(task.nodeId);
            } else {
                // Add the node to external_available_nodes for the LM and partition
                this.externalAvailableNodes[task.lm][task.partitionId].push(task.nodeId);
            }
        } else {
            console.log(`Task queue is not empty. Not updating available nodes.`);
        }

        return { status: "success" };
    }


    updateStatus(status, lmId) {
        Object.entries(status).forEach(([partitionId, nodes]) => {
            // Check if the partitionId matches gmId
            if (partitionId === this.gmId) {
                // Update the internal available nodes
                this.internalAvailableNodes[lmId] = nodes;
                console.log(`Internal update for ${partitionId}:`, this.internalAvailableNodes[partitionId]);
            } else {
                // Initialize the externalAvailableNodes[lmId] if it doesn't exist
                if (!this.externalAvailableNodes[lmId]) {
                    this.externalAvailableNodes[lmId] = {};
                }
                // Update the external available nodes for the given lmId
                this.externalAvailableNodes[lmId][partitionId] = nodes;
                console.log(`External update for ${partitionId} under LM ${lmId}:`, this.externalAvailableNodes[lmId][partitionId]);
            }
        });

        // Return success status
        return { status: "success" };
    }


    unscheduleTask(task) {
        // console.log("unscheduled task:", task);

        this.taskQueue.unshift(task);
        console.log(`Task ${task.taskId} re-added to queue due to verification failure`);
        return { status: "success" };
    }

    run(port = 5011) {
        this.app.listen(port, () => {
            console.log(`GM server running on port ${port}`);
        });
    }
}

// Example usage
const manager = new DataCenterManager();
manager.run();

module.exports = DataCenterManager;