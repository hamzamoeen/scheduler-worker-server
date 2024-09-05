const cron = require('node-cron');
const { getSchedulersAtSpecificTime } = require('./scheduler-functions');
const moment = require('moment-timezone');


// Define an async function to run your code
async function runScheduler() {
  await getSchedulersAtSpecificTime();
}

// Schedule the cron job to run at a specific time, e.g., every day at 8:20 PM
cron.schedule('* * * * *', async () => {
  try {
    const currentTimeInTimezone = moment().tz('Asia/Karachi');
    console.log("scheduler.js --->>> currentTimeInTimezone ---->>> ", currentTimeInTimezone);
    await runScheduler();
  } catch (error) {
    console.error('Error in cron job:', error);
  }
});
