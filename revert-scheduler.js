const cron = require('node-cron');
const { getRevertSchedulersAtSpecificTime } = require('./scheduler-functions');
const moment = require('moment-timezone');

// Define an async function to run your code
async function runRevertScheduler() {
    console.log('revert-scheduler.js --->>> Starting Cron job:');
    await getRevertSchedulersAtSpecificTime();
}

// Schedule the cron job to run at a specific time, e.g., every day at 8:20 PM
cron.schedule('* * * * *', async () => {
  try {
    const currentTimeInTimezone = moment().tz('Asia/Karachi');
    console.log("revert-scheduler.js --->>> currentTimeInTimezone ---->>> ", currentTimeInTimezone);
    await runRevertScheduler();
  } catch (error) {
    console.error('revert-scheduler.js --->>> Error in cron job:', error);
  }
});
