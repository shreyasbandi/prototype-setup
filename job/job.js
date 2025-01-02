const fs = require('fs');
const axios = require('axios');

// Define the threshold value
const THRESHOLD = 90.58;

// Job counter for unique job IDs
let jobCounter = 0;

// Define the Job class
class Job {
    constructor(jobId, arrivalTime, numTasks, maxExecutionTime, jobType, executionTimes) {
        this.jobId = jobId; // Unique identifier for the job
        this.arrivalTime = arrivalTime;
        this.numTasks = numTasks;
        this.executionTimes = executionTimes;
        this.jobType = jobType;
        this.maxExecutionTime = maxExecutionTime;
    }
}

// Function to dynamically discover GM pods
// function getGMPodConfig() {
//     const gmServiceName = process.env.GM_SERVICE_NAME || 'gm-service'; // Base service name
//     const gmServiceNamespace = process.env.GM_NAMESPACE || 'default'; // Namespace (default if not provided)
//     const gmServicePort = process.env.GM_SERVICE_PORT || '5011'; // Port number
//     const gmReplicas = parseInt(process.env.GM_REPLICAS || '2', 10); // Number of replicas
//     const gmConfig = {};

//     console.log(`GM Service Name: ${gmServiceName}`);
//     console.log(`Namespace: ${gmServiceNamespace}`);
//     console.log(`Port: ${gmServicePort}`);
//     console.log(`Replicas: ${gmReplicas}`);

//     // Generate addresses for each GM replica
//     for (let i = 0; i < gmReplicas; i++) {
//         const gmAddress = `${gmServiceName}-${i}-service.${gmServiceNamespace}.svc.cluster.local:${gmServicePort}`;
//         gmConfig[`gm-${i}`] = { address: gmAddress };
//         console.log(`Added GM pod address: ${gmAddress}`);
//     }
    
//     return gmConfig;
// }

function getGMPodConfig() {
    const gmServiceName = process.env.GM_SERVICE_NAME || 'gm-service';
    const gmNamespace = process.env.GM_NAMESPACE || 'default';
    const gmServicePort = process.env.GM_SERVICE_PORT || '5011';
    const gmReplicas = parseInt(process.env.GM_REPLICAS || '2', 10);

    const gmConfig = {};
    for (let i = 0; i <gmReplicas; i++) {
        // DNS format for StatefulSet pod
        const gmAddress = `gm-${i}.${gmServiceName}.${gmNamespace}.svc.cluster.local:${gmServicePort}`;
        gmConfig[`gm-${i}`] = { address: gmAddress };
    }
    return gmConfig;
}

// Function to randomly select a GM
function selectRandomGM(gmConfig) {
    const gmIds = Object.keys(gmConfig); // Get all GM IDs
    const randomIndex = Math.floor(Math.random() * gmIds.length); // Random index
    return gmConfig[gmIds[randomIndex]]; // Return the selected GM
}

function classifyJobs(traceFile) {
    const startTime = Date.now();

    // Dynamically fetch GM configuration
    const gmConfig = getGMPodConfig();

    // Read the trace file
    fs.readFile(traceFile, 'utf8', (err, data) => {
        if (err) {
            console.error(`Error reading trace file: ${err.message}`);
            return;
        }

        // Process each line in the trace file
        const lines = data.trim().split('\n');
        lines.forEach((line) => {
            const parts = line.trim().split(/\s+/);

            // Extract arrival time, number of tasks, and execution times
            const arrivalTime = parseFloat(parts[0]);
            const numTasks = parseInt(parts[1], 10);
            const executionTimes = parts.slice(3).map(Number);

            // Ensure the number of tasks matches the execution times
            if (executionTimes.length !== numTasks) {
                console.error(
                    `Error: Mismatch in task count and execution times at arrival time ${arrivalTime}`
                );
                return;
            }

            // Wait until the job's arrival time in real time
            const elapsedTime = (Date.now() - startTime) / 1000; // Convert to seconds
            const delay = arrivalTime - elapsedTime;
            if (delay > 0) {
                setTimeout(() => {
                    processJob(arrivalTime, numTasks, executionTimes, gmConfig);
                }, delay * 1000); // Convert delay to milliseconds
            } else {
                processJob(arrivalTime, numTasks, executionTimes, gmConfig);
            }
        });
    });
}

function processJob(arrivalTime, numTasks, executionTimes, gmConfig) {
    // Compute the maximum execution time
    const maxExecutionTime = Math.max(...executionTimes);

    // Classify the job
    const jobType = maxExecutionTime > THRESHOLD ? 'Long' : 'Short';

    // Increment the job counter and create a unique jobId
    const jobId = `JOB-${++jobCounter}`;

    // Create a Job object
    const job = new Job(jobId, arrivalTime, numTasks, maxExecutionTime, jobType, executionTimes);

    // Randomly select a GM
    const selectedGM = selectRandomGM(gmConfig);

    // Construct the GM endpoint
    const gmEndpoint = `http://${selectedGM.address}`;

    // Send the job to the GM
    sendJobToGM(gmEndpoint, job);
}

function sendJobToGM(gmEndpoint, job) {
    console.log('Sending job:', JSON.stringify(job, null, 2)); // Log the job object for verification

    axios
        .post(`${gmEndpoint}/assign_job`, {
            headers: {
                'Content-Type': 'application/json', // Ensure correct content type
            },
            job
        })
        .then((response) => {
            console.log(`Job ${job.jobId} successfully sent to GM at ${gmEndpoint}`);
        })
        .catch((error) => {
            if (error.response) {
                console.error(
                    `Error response from GM: ${error.response.status} - ${JSON.stringify(error.response.data, null, 2)}`
                );
            } else {
                console.error(`Failed to send Job ${job.jobId} to GM at ${gmEndpoint}: ${error.message}`);
            }
        });
}

// Replace with the actual file path
const traceFilePath = process.env.TRACE_FILE || 'own.tr'; // Trace file path from environment variable

// Start job classification
classifyJobs(traceFilePath);
