const axios = require('axios');
const {pool} = require('./db');
const moment = require('moment-timezone');


require('dotenv').config();

async function getSchedulersAtSpecificTime() {
    try {
      // Establish the database connection
      let sql = `SELECT * FROM schedulers WHERE scheduler_status = 'scheduled' and change_prices_datetime IS NOT NULL `;
      let [results] = await pool.execute(sql);

      console.log("Results --->> getSchedulersAtSpecificTime(): ", results);
      
      // Filter events based on the current time in their timezone
      const matchingEvents = results.filter(event => {
        const currentTimeInTimezone = moment().tz(event.timezone);
        let changePricesDatetime = event.change_prices_datetime;
        if(changePricesDatetime == '') return;

        // Extract hour and minute
        let hour = changePricesDatetime.getHours();
        let minute = changePricesDatetime.getMinutes();

        let month = (changePricesDatetime.getMonth() + 1); // Returns 0-11 for January-December
        let day = changePricesDatetime.getDate();    // Returns 1-31 for the day of the month
        let year = changePricesDatetime.getFullYear(); // Returns the full year (e.g., 2023)

        console.log(`currentTimeInTimezone --->>> ${event.timezone}: `, currentTimeInTimezone);        
        console.log(`From DB Datetime --->>> day: ${day}, month: ${month}, year: ${year}, Hour: ${hour}, Minute: ${minute}: `);
        return (
          currentTimeInTimezone.hour() == hour &&
          currentTimeInTimezone.minute() == minute && 
          (currentTimeInTimezone.getMonth() + 1) == month && 
          currentTimeInTimezone.getDate() == day && 
          currentTimeInTimezone.getFullYear() == year 
        );
      });
  
      console.log('Matching Events ---->>> getSchedulersAtSpecificTime() :', matchingEvents);
      for (let i = 0; i < matchingEvents.length; i++) {
        console.log("scheduler scheduler", scheduler);
        let scheduler = matchingEvents[i];
        let session = JSON.parse(scheduler.session_data);
        await runSchedulerJob(session, scheduler.id);        
      }

    } catch (error) {
      console.error('Error occurred getSchedulersAtSpecificTime():', error.message);
    } finally {
        // pool.release();
    }
}

async function getRevertSchedulersAtSpecificTime() {
    try {
      // Establish the database connection
      let sql = `SELECT * FROM schedulers WHERE scheduler_status = 'reversionScheduled' and revert_to_original_price = 1 and revert_to_original_price_datetime IS NOT NULL `;
      let [results] = await pool.execute(sql);
  
      // Filter events based on the current time in their timezone
      const matchingEvents = results.filter(event => {
        const currentTimeInTimezone = moment().tz(event.timezone);
        let changePricesDatetime = event.revert_to_original_price_datetime;
        console.log("changePricesDatetime--->", changePricesDatetime);
        if(changePricesDatetime == '') return;
        // Extract hour and minute
        let hour = changePricesDatetime.getHours();
        let minute = changePricesDatetime.getMinutes();
        console.log(`getRevertSchedulersAtSpecificTime --->>> currentTimeInTimezone --->>> ${event.timezone}: `, currentTimeInTimezone);        
        console.log(`getRevertSchedulersAtSpecificTime --->>> From DB Datetime --->>> Hour: ${hour}, Minute: ${minute}: `);
        return (
          currentTimeInTimezone.hour() == hour &&
          currentTimeInTimezone.minute() == minute
        );
      });
  
      console.log('Matching Events ---->> getRevertSchedulersAtSpecificTime() :', matchingEvents);
      for (let i = 0; i < matchingEvents.length; i++) {
        let scheduler = matchingEvents[i];
        let session = JSON.parse(scheduler.session_data);
        await runRevertSchedulerJob(session, scheduler.id);        
      }

    } catch (error) {
      console.error('Error occurred getRevertSchedulersAtSpecificTime():', error.message);
    } finally {
        // pool.release();
    }
}



async function updateSchedulerStatus(scheduler_id, data){
    let sql = `UPDATE schedulers SET ? WHERE id = ${scheduler_id} `;
    const [result] = await pool.query(sql, [data]);
}

async function getSchedulerJobs(session, scheduler_id = null) {
    const { shop, accessToken } = session;
    // scheduler_id = 2;
    console.log("getSchedulerJobs is in progress --->>> ");
    try {
        let sql = `SELECT s.*, 
                        st.scheduler_id, 
                        st.is_add, 
                        st.is_remove, 
                        st.tag_name, 

                        sp.scheduler_id,
                        sp.shopify_proudct_id,
                        sp.shopify_variant_id,
                        sp.shopify_product_title,
                        sp.shopify_product_variant_title,
                        sp.product_old_price,
                        sp.product_new_price,
                        sp.product_old_compare_at_price,
                        sp.product_new_compare_at_price,
                        sp.shopify_variant_image,
                        sp.shopify_custom_variant_title, 

                        sbp.scheduler_id,
                        sbp.price_change_job_column,
                        sbp.price_change_amount,
                        sbp.price_rounding,
                        sbp.certain_number_amount,
                        sbp.compare_at_change_job_column,
                        sbp.compare_at_change_amount,
                        sbp.compare_at_rounding,
                        sbp.certain_ca_number_amount,

                        sf.scheduler_id,
                        sf.columnss,
                        sf.relations,
                        sf.conditions
        FROM schedulers s
        LEFT JOIN scheduled_products sp on s.id = sp.scheduler_id and sp.shopify_proudct_id IS NOT NULL
        LEFT JOIN scheduler_bulk_price sbp on s.id = sbp.scheduler_id
        LEFT JOIN scheduler_filters sf on s.id = sf.scheduler_id
        LEFT JOIN scheduler_tags st on s.id = st.scheduler_id
        WHERE s.shop_name = '${shop}' `;

        if (scheduler_id) {
            sql += ` AND s.id = ${scheduler_id} `;
        }

        sql += ` order by s.id desc`;

        let [results] = await pool.execute(sql);

        const jobs = [];
        let currentJobId = null;
        let currentJob = null;

        results.sort((a, b) => a.id - b.id); // Sort by job ID

        results.forEach(item => {
            if (currentJobId !== item.id) {
                // Finalize the current job and start a new one
                currentJobId = item.id;

                currentJob = {
                    scheduledFilters: [],
                    selectionCriteria: item.product_selection_type || "",
                    selectedCollections: "",
                    method: item.price_rule_type || "",
                    changePriceType: item.price_change_job_column || "",
                    price: item.price_change_amount || 0,
                    priceRoundingRounding: item.price_rounding || "",
                    roundingValue: item.certain_number_amount || 0,
                    changeCAPriceType: item.compare_at_change_job_column || "",
                    compareAt: item.compare_at_change_amount || 0,
                    compareAtRoundingRounding: item.compare_at_rounding || "",
                    scheduledProducts: [],
                    addTagsChecked: false,
                    removeTagsChecked: false,
                    selectedTags: [],
                    selectedRemovedTags: [],
                    changePricesLater: item.change_prices_time || "",
                    revertToOriginalPricesLater: item.revert_to_original_price === 1,
                    scheduledTime: item.change_prices_datetime ? new Date(item.change_prices_datetime).toLocaleTimeString('en-GB', { timeZone: item.timezone }) : "",
                    scheduledDate: item.change_prices_datetime ? new Date(item.change_prices_datetime).toDateString() : "",
                    revertTime: item.revert_to_original_price_datetime ? new Date(item.revert_to_original_price_datetime).toLocaleTimeString('en-GB', { timeZone: item.timezone }) : "",
                    revertDate: item.revert_to_original_price_datetime ? new Date(item.revert_to_original_price_datetime).toDateString() : "",
                    timezone: item.timezone || "",
                    themeChangeWhenThisPriceChangeIsTriggered: !!item.change_theme_id,
                    chooseTheTheme: item.change_theme_id || "",
                    revertToTheSpecificThemeLater: !!item.revert_theme_id || "",
                    chooseTheRollbackTheme: item.revert_theme_id || "",
                    jobName: item.scheduler_name || "",
                    jobId: item.id,
                    currentDateTime: item.current_date_time ? new Date(item.current_date_time).toDateString() : "",// Added this line to get the current date (Developer2) Please approve
                    roundingCaValueRB: item.certain_ca_number_amount || 0, // Added this line to get the reounding number for compare at (Developer2) Please approve
                };

                jobs.push(currentJob);
            }
            // Ensure each filter is only added once
            if (item.columnss && item.relations && item.conditions) {
                if (!currentJob.scheduledFilters.find(filter => filter.type === item.columnss && filter.condition === item.relations && filter.content === item.conditions)) {
                    currentJob.scheduledFilters.push({
                        type: item.columnss || "",
                        condition: item.relations || "",
                        content: item.conditions || ""
                    });
                }
            }

            // Ensure each product is only added once

            if (item.shopify_variant_id) {
                if (!currentJob.scheduledProducts.find(product => product.id === item.shopify_variant_id)) {
                    currentJob.scheduledProducts.push({
                        id: item.shopify_variant_id || "",
                        title: item.shopify_custom_variant_title || "", //Need Title that was concatenated from product and variant (Developer2) Please approve
                        sku: "",
                        price: item.product_old_price ? item.product_old_price.toFixed(2) : "",
                        compareAtPrice: item.product_old_compare_at_price ? item.product_old_compare_at_price.toFixed(2) : "",
                        inventoryQuantity: 0,
                        image: item.shopify_variant_image || "", //Need Image Url (Developer2) Please approve
                        shopifyProductId: item.shopify_proudct_id || "",
                        shopifyProductTitle: item.shopify_product_title || "",
                        shopifyVariantTitle: item.shopify_product_variant_title || "",
                        modifiedCompareAtPrice: item.product_new_compare_at_price || "",
                        modifiedPrice: item.product_new_price ? item.product_new_price.toFixed(2) : ""
                    });
                }
            }

            // Ensure each tag is only added once
            if (item.tag_name) {
                if (item.is_add) {
                    currentJob.addTagsChecked = true;
                    if (!currentJob.selectedTags.includes(item.tag_name)) {
                        currentJob.selectedTags.push(item.tag_name || "");
                    }
                } else if (item.is_remove) {
                    currentJob.removeTagsChecked = true;
                    if (!currentJob.selectedRemovedTags.includes(item.tag_name)) {
                        currentJob.selectedRemovedTags.push(item.tag_name || "");
                    }
                }
            }
        });

        // for debug, this below code can be uncommented
        // const filePath = path.join(__dirname, 'get_schedule_datas.json'); // Resolve the file path        
        // fs.writeFile(filePath, JSON.stringify(jobs, null, 2), (err) => {
        //     if (err) {
        //         console.error('Error writing to JSON file:', err);
        //     } else {
        //         console.log('JSON file has been saved.');
        //     }
        // });

        return jobs;

    } catch (err) {
        console.error('Error inserting data, transaction rolled back:', err);
        // const filePath = path.join(__dirname, 'get_schedule_datas.json'); // Resolve the file path        
        // fs.writeFile(filePath, JSON.stringify(err.sql, null, 2), (err) => {
        //     if (err) {
        //         console.error('Error writing to JSON file:', err);
        //     } else {
        //         console.log('JSON file has been saved.');
        //     }
        // });
        throw err;
    }
    // finally {
    //     pool.release();
    // }
}

async function runSchedulerJob(session, scheduler_id) {
    if (!scheduler_id) {
        console.log("Scheduler ID required!.");
        throw new Error("Scheduler ID is required.");
    }

    // await updateSchedulerStatus(scheduler_id, {scheduler_status: 'inProgress'});

    let scheduler = await getSchedulerJobs(session, scheduler_id);
    if (!scheduler?.length) {
        throw new Error("No Scheduler Found.");
    }

    console.log("Scheduler Data ---->>> runSchedulerJob() ", scheduler);
    let scheduledProducts = scheduler[0].scheduledProducts;

    if (scheduledProducts?.length) {
        const uniqueProductIds = await uniqueByShopifyProductId(scheduledProducts);
        await updateTagsForProducts(session, uniqueProductIds, scheduler[0].selectedTags, 'add');
        await updateVariantPrice(session, scheduledProducts, 'update');
        await updateTagsForProducts(session, uniqueProductIds, scheduler[0].selectedRemovedTags, 'remove');
    }

    if (scheduler[0].chooseTheTheme && scheduler[0].themeChangeWhenThisPriceChangeIsTriggered) {
        await publishAndRevertTheme(session, scheduler[0].chooseTheTheme);
    }

    if(scheduler[0]?.revertToOriginalPricesLater && scheduler[0]?.revertDate != ''){
        await updateSchedulerStatus(scheduler_id, {scheduler_status: 'reversionScheduled'});
    }else{
        await updateSchedulerStatus(scheduler_id, {scheduler_status: 'scheduledCompleted'});
    }

    return scheduler;
}

async function runRevertSchedulerJob(session, scheduler_id) {
    if (!scheduler_id) {
        throw new Error("Scheduler ID is required.");
    }

    let scheduler = await getSchedulerJobs(session, scheduler_id);
    if (!scheduler?.length) {
        throw new Error("No Scheduler Found.");
    }

    await updateSchedulerStatus(scheduler_id, {scheduler_status: 'reversionInprogress'});

    console.log("Scheduler Data ---->>> runRevertSchedulerJob() ", scheduler[0]);
    let scheduledProducts = scheduler[0].scheduledProducts;

    if (scheduledProducts?.length) {
        const uniqueProductIds = await uniqueByShopifyProductId(scheduledProducts);
        await updateTagsForProducts(session, uniqueProductIds, scheduler[0].selectedRemovedTags, 'add');
        await updateVariantPrice(session, scheduledProducts, 'revert');
        await updateTagsForProducts(session, uniqueProductIds, scheduler[0].selectedTags, 'remove');
    }

    if (scheduler[0].chooseTheRollbackTheme && scheduler[0].revertToTheSpecificThemeLater) {
        await publishAndRevertTheme(session, scheduler[0].chooseTheRollbackTheme);
    }

    await updateSchedulerStatus(scheduler_id, {scheduler_status: 'completed'});

    return scheduler;
}

async function updateTagsForProducts(session, uniqueProductIds, tags, action) {
    const promises = uniqueProductIds.map(async ({ shopifyProductId }) => {
        let currentTags = await fetchCurrentTags(session, shopifyProductId);
        let updatedTags;

        if (action === 'add') {
            updatedTags = Array.from(new Set([...currentTags, ...tags]));
        } else {
            updatedTags = currentTags.filter(tag => !tags.includes(tag));
        }

        console.log(`Product ${shopifyProductId} - ${action === 'add' ? 'Adding' : 'Removing'} Tags:`, updatedTags);

        if (updatedTags.length) {
            return updateTags(session, shopifyProductId, updatedTags);
        }
    });

    await Promise.all(promises);
}

async function updateVariantPrice(session, products, action) {
    const promises = products.map(async product => {
        let priceData = {
            id: product.id,
            price: action === 'update' ? (product.modifiedPrice || 0) : (product.price || 0),
            compareAtPrice: action === 'update' ? (product.modifiedCompareAtPrice || null) : (product.compareAtPrice || null)
        };

        console.log(`Product ${product.id} - ${action === 'update' ? 'Updating' : 'Reverting'} Prices:`, priceData);
        return updateVariantPrices(session, priceData);
    });

    await Promise.all(promises);
}


const updateVariantPrices = async (session, single) => {
    const { shop, accessToken } = session;
    const endpoint = `https://${shop}/admin/api/2024-01/graphql.json`;

    const query = `
        mutation updateProductVariant($input: ProductVariantInput!) {
        productVariantUpdate(input: $input) {
            productVariant {
            id
            price
            compareAtPrice
            }
            userErrors {
            field
            message
            }
        }
        }
    `;

    const variables = {
        input: {
            id: single.id,
            price: single?.price,
            compareAtPrice: single?.compareAtPrice
        }
    };

    try {
        const response = await axios.post(
            endpoint,
            {
                query,
                variables
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                    'X-Shopify-Access-Token': accessToken
                }
            }
        );

        const { data } = response;
        if (data.errors) {
            console.error('Errors:', data.errors);
        } else if (data.data.productVariantUpdate.userErrors.length > 0) {
            console.error('User Errors:', data.data.productVariantUpdate.userErrors);
        } else {
            console.log('Updated variant:', data.data.productVariantUpdate.productVariant);
        }
    } catch (error) {
        console.error('Request failed:', error);
    }
};

const publishAndRevertTheme = async (session, themeId) => {
    const { shop, accessToken } = session;

    const endpoint = `https://${shop}/admin/api/2023-01/themes/${themeId}.json`;

    const payload = {
        theme: {
            id: themeId,
            role: 'main'
        }
    };

    try {
        const response = await axios.put(
            endpoint,
            payload,
            {
                headers: {
                    'Content-Type': 'application/json',
                    'X-Shopify-Access-Token': accessToken
                }
            }
        );

        if (response.data.errors) {
            console.error('Errors:', response.data.errors);
        } else {
            console.log('Published theme:', response.data.theme);
        }
    } catch (error) {
        console.error('Request failed:', error);
    }
};

async function fetchCurrentTags(session, productId) {
    const {shop, accessToken} = session;
  const query = `
    query {
      product(id: "${productId}") {
        tags
      }
    }
  `;

  const response = await axios.post(`https://${shop}/admin/api/2023-01/graphql.json`, 
    { query }, 
    {
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': accessToken,
      },
    }
  );

  if (response.data.errors) {
    throw new Error('Error fetching product tags: ' + response.data.errors);
  }

  return response.data.data.product.tags;
}

// Function to update tags
async function updateTags(session, productId, newTags) {
    const {shop, accessToken} = session;
  const query = `
    mutation productTagsUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          tags
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const variables = {
    input: {
      id: productId,
      tags: newTags,
    },
  };

  const response = await axios.post(`https://${shop}/admin/api/2023-01/graphql.json`, 
    { query, variables }, 
    {
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': accessToken,
      },
    }
  );

  if (response.data.errors) {
    throw new Error('Error updating tags: ' + response.data.errors);
  }

  console.log("Response From Tags Update Function: ",response?.data?.data?.productUpdate?.product?.tags);
  return response?.data?.data?.productUpdate?.product?.tags;
}

const uniqueByShopifyProductId = async (data) => {
    const seenIds = new Set();
    return data.filter(item => {
      if (seenIds.has(item.shopifyProductId)) {
        return false;
      } else {
        seenIds.add(item.shopifyProductId);
        return true;
      }
    });
};


module.exports = {
    getSchedulersAtSpecificTime,
    getRevertSchedulersAtSpecificTime
};
