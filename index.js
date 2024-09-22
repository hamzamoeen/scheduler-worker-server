const { Worker } = require('bullmq');
const Redis = require('ioredis');
const { getSchedulerJobByID, runSchedulerJob, runRevertSchedulerJob } = require('./scheduler-functions-for-redis');

// Redis connection
const redisConnectionUrl = process.env.REDIS_URL || 'rediss://default:VJktXv52bNTZZB6ilOdY4ojkruZqmHwUv6mAJccd1CDrurowRQOFmkca8GLikizq@so89sw.stackhero-network.com:10008';
const redisOptions = {
    maxRetriesPerRequest: null, // Required by BullMQ
};

const redis = new Redis(redisConnectionUrl, redisOptions);

// Create the worker to process both 'scheduleUpdateJob' and 'revertUpdateJob'
const worker = new Worker(
    'scheduler',
    async job => {
        const { jobId } = job.data;
        
        if (job.name === 'scheduleUpdateJob') {
            console.log(`Processing scheduled update for job ${jobId} at ${new Date().toISOString()}`);
            await runScheduledUpdateTask(jobId);  // Run the product update task
        }

        if (job.name === 'revertUpdateJob') {
            console.log(`Processing revert for job ${jobId} at ${new Date().toISOString()}`);
            await runRevertUpdateTask(jobId);  // Run the revert task
        }
    },
    { connection: redis }
);

// Event listener for completed jobs
worker.on('completed', (job) => {
    console.log(`Job ${job.id} (${job.name}) completed successfully.`);
});

// Event listener for failed jobs
worker.on('failed', (job, err) => {
    console.error(`Job ${job.id} (${job.name}) failed with error: ${err.message}`);
});

// Your task logic (replace these with actual tasks)
async function runScheduledUpdateTask(jobId) {
    let data = await getSchedulerJobByID(jobId);
    console.log(`data for JobId = ${jobId} ----->>>> runScheduledUpdateTask(): `, data);
    if(data?.length > 0){
        let session = {
            shop: data?.shop_name,
            accessToken: data?.access_token
        };        
        await runSchedulerJob(session, jobId);
    }
    console.log(`Running scheduled product update for job ${jobId}`);
}

async function runRevertUpdateTask(jobId) {
    let data = await getSchedulerJobByID(jobId);
    console.log(`data for JobId = ${jobId} ----->>>> runRevertUpdateTask(): `, data);
    if(data?.length > 0){
        let session = {
            shop: data?.shop_name,
            accessToken: data?.access_token
        };        
        await runRevertSchedulerJob(session, jobId);
    }
    console.log(`Running revert product update for job ${jobId}`);
}



// require('dotenv').config();

// const express = require('express');
// const app = express();

// require('./schedulers');
// require('./revert-scheduler');

// const PORT = process.env.PORT || 3000;

// app.get('/', (req, res) => {
//   res.send('Hello, World!');
// });

// app.listen(PORT, () => {
//   console.log(`Server is running on port ${PORT}`);
// });
