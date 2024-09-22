const axios = require('axios');
const {pool} = require('./db');
const moment = require('moment-timezone');
const { sendEmailNotification } = require('./email');
const path = require('path');
const { fileURLToPath } = require('url');




// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);

// Convert date and time with timezone to UTC and format as 'YYYY-MM-DD HH:MM:SS'
const convertToUTC = (dateString, timeString, timezone) => {
    const dateTimeInTZ = moment(`${formatDateForMySQL(dateString)} ${timeString}`);
    const dateTimeInUTC = dateTimeInTZ.utc();
    return dateTimeInUTC.format('YYYY-MM-DD HH:mm:ss');
};

const formatDateForMySQL = (dateString) => {
    const [month, day, year] = dateString.split(' ').slice(1, 4);
    const monthMap = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    };
    const monthNum = monthMap[month];
    return `${year}-${monthNum}-${day}`;
};

// Format time string in 'HH:MM:SS' format
const formatTimeForMySQL = (timeString) => {
    return `${timeString}:00`;
};


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
                        sp.tags_before,

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

        console.log("SSSSSQLE : ", sql);

        let [results] = await pool.execute(sql);
        //console.log('SQL Results:', results);

        const jobs = [];
        let currentJobId = null;
        let currentJob = null;

        results.sort((a, b) => a.id - b.id); // Sort by job ID

        results.forEach(item => {
            if (currentJobId !== item.id) {
                // Finalize the current job and start a new one
                currentJobId = item.id;

                currentJob = {
                    scheduledFilters: {
                        appliedDisjunctively: (item?.appliedDisjunctively && item?.appliedDisjunctively == 1) ? true : null,
                        rules: []
                    },
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
                    themeRoute: item.only_theme_job === 1 ? 1 : 0,
                    jobId: item.id,
                    currentDateTime: item.current_date_time ? new Date(item.current_date_time).toDateString() : "",// Added this line to get the current date (Developer2) Please approve
                    roundingCaValueRB: item.certain_ca_number_amount || 0, // Added this line to get the reounding number for compare at (Developer2) Please approve
                    jobStatus: item.scheduler_status || null,
                    appliedDisjunctively: (item?.appliedDisjunctively && item?.appliedDisjunctively == 1) ? true : null,
                    selectedCSVFormat: item.selected_csv_format || null,
                    shopName: item.shop_name || "",
                    changePricesDateTimeUTC: item.change_prices_datetime || null,
                    revertToOriginalPriceDateTimeUTC: item.revert_to_original_price_datetime || null

                };

                jobs.push(currentJob);
            }

            // Ensure each filter is only added once
            if (item.columnss && item.relations && item.conditions) {
                if (!currentJob?.scheduledFilters?.rules?.length || !currentJob?.scheduledFilters?.rules.find(filter => filter.type === item.columnss && filter.condition === item.relations && filter.content === item.conditions)) {
                    currentJob.scheduledFilters.rules.push({
                        type: item.columnss || "",
                        condition: item.relations || "",
                        content: item.conditions || ""
                    })
                }
            }



            // Ensure each product is only added once

            if (item.shopify_variant_id) {
                if (!currentJob.scheduledProducts.find(product => product.id === item.shopify_variant_id)) {
                    // Debugging: Print the tags_before value
                    let tagsBefore = [];
                    try {
                        tagsBefore = Array.isArray(item.tags_before)
                            ? item.tags_before
                            : JSON.parse(item.tags_before || '[]');
                        if (!Array.isArray(tagsBefore)) throw new Error();
                    } catch (e) {
                        console.error('Error parsing tags_before:', e.message);
                        tagsBefore = [];
                    }
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
                        modifiedPrice: item.product_new_price ? item.product_new_price.toFixed(2) : "",
                        tags: tagsBefore,  // Tags before run job
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



async function getSchedulerJobByID(jobId) {
    console.log("getSchedulerJobs is in progress --->>> ");
    try {
        let sql = `SELECT * FROM schedulers where id = ${jobId}`;
        let [results] = await pool.execute(sql);
        //console.log('SQL Results:', results);
        return results;
    } catch (err) {
        console.error('Error getting data, transaction rolled back:', err);
        throw err;
    }
    // finally {
    //     pool.release();
    // }
}

async function deleteScheduleById(session, scheduler_id) {
    const { shop, accessToken } = session;

    const connection = await pool.getConnection();

    try {
        if (!scheduler_id) {
            console.log("Scheduler ID required!.");
            throw new Error("Scheduler ID is required.");
        }

        await connection.beginTransaction();

        let delete_sql = `DELETE FROM schedulers WHERE id = ${scheduler_id} and shop_name = '${shop}' `;
        await connection.query(delete_sql);

        await connection.commit();

    } catch (err) {
        await connection.rollback();
        console.error('Error Deleting data, transaction rolled back:', err);
        throw err; // Re-throw the error to handle it further up if needed
    } finally {
        connection.release();
    }

}

async function runSchedulerJob(session, scheduler_id) {
    let errorOccurred = false;
    if (!scheduler_id) {
        console.log("Scheduler ID required!");
        throw new Error("Scheduler ID is required.");
    }

    let scheduler = await getSchedulerJobs(session, scheduler_id);
    if (!scheduler?.length) {
        throw new Error("No Scheduler Found.");
    }

    await updateSchedulerStatus(scheduler_id, { scheduler_status: 'inProgress' });
    try {
        console.log("Job Started");
        let scheduledProducts = scheduler[0].scheduledProducts;

        if (scheduledProducts?.length) {
            const uniqueProductIds = await uniqueByShopifyProductId(scheduledProducts);
            await retryWithBatchReduction(async (batchSize, resumeFromId) => {
                await updateTagsForProducts(session, uniqueProductIds, scheduler[0].selectedTags, 'add', batchSize, resumeFromId);
            });
            await retryWithBatchReduction(async (batchSize, resumeFromId) => {
                await updateVariantPrice(session, scheduledProducts, 'update', batchSize, resumeFromId);
            });
            await retryWithBatchReduction(async (batchSize, resumeFromId) => {
                await updateTagsForProducts(session, uniqueProductIds, scheduler[0].selectedRemovedTags, 'remove', batchSize, resumeFromId);
            });
        }

        if (scheduler[0].chooseTheTheme && scheduler[0].themeChangeWhenThisPriceChangeIsTriggered) {
            await publishAndRevertTheme(session, scheduler[0].chooseTheTheme);
        }

        if (scheduler[0]?.revertToOriginalPricesLater && scheduler[0]?.revertDate !== '') {
            await updateSchedulerStatus(scheduler_id, { scheduler_status: 'reversionScheduled' });
        } else {
            await updateSchedulerStatus(scheduler_id, { scheduler_status: 'scheduledCompleted' });
        }
        console.log("Job Completed");
    } catch (error) {
        console.error("Error in runSchedulerJob:", error);
        errorOccurred = true;
        await updateSchedulerStatus(scheduler_id, { scheduler_status: 'Pending Retry' });
        throw error;
    }

    // Only send email if no error occurred and not in retry status
    if (!errorOccurred && scheduler[0].scheduler_status !== 'Pending Retry') {
        const testEmailData = {
            shopEmailAddress: "expertifies@gmail.com",
            jobName: scheduler[0].jobName || "Job Name",
            jobOperation: 'Completed',
            scheduler: scheduler[0],
        };
        await sendEmailNotification(testEmailData);
    }

    return scheduler;
}

async function updateSchedulerStatus(scheduler_id, data) {
    let sql = `UPDATE schedulers SET ? WHERE id = ${scheduler_id} `;
    const [result] = await pool.query(sql, [data]);
}

// Retry mechanism function with exponential backoff
async function retryWithBatchReduction(asyncFunction, maxRetries = 7, initialBatchSize = 8, resumeFromId = null) {
    let retries = 0;
    let batchSize = initialBatchSize;
    let delayTime = 1000; // Initial delay of 1 seconds
    let lastProcessedId = resumeFromId; // Track last processed ID

    while (retries < maxRetries) {
        try {
            console.log(`Attempt ${retries + 1} with batch size ${batchSize} and resumeFromId ${resumeFromId}`);
            lastProcessedId = await asyncFunction(batchSize, resumeFromId);
            break;  // If the function executes successfully, exit the loop
        } catch (error) {
            if (error.response?.status === 429) { // Check if the error is due to throttling hitting api llimit
                await delay(delayTime);
                retries++;
                delayTime *= 2;
                console.log(`Rate limit exceeded, retrying in ${delayTime}ms...`);
                resumeFromId = error.shopifyProductId || lastProcessedId;
                batchSize = Math.max(1, batchSize - 1);
            } else { // 502 or 503 || so far 502 Bad Gateway or shopify down have no control over it {Might ne auth lost on dev or user error}
                await delay(delayTime);
                retries++;
                delayTime *= 2; // Exponential backoff
                resumeFromId = error.shopifyProductId || lastProcessedId;
                console.log(`${error.response?.message}, retrying in ${delayTime}ms...`);
                batchSize = batchSize == 1 ? batchSize : Math.max(1, batchSize - 1);
            }
        }
    }

    if (retries >= maxRetries) {
        throw new Error(`Operation failed after ${maxRetries} retries.`);
    }
    return lastProcessedId;
}

// Modified delay function for retry/backoff
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
async function runRevertSchedulerJob(session, scheduler_id) {
    let errorOccured = false;
    if (!scheduler_id) {
        throw new Error("Scheduler ID is required.");
    }
    let scheduler = await getSchedulerJobs(session, scheduler_id);
    if (!scheduler?.length) {
        throw new Error("No Scheduler Found.");
    }
    await updateSchedulerStatus(scheduler_id, { scheduler_status: 'reversionInprogress' });
    try {
        //console.log("Scheduler Data ---->>> runRevertSchedulerJob() ", scheduler[0]);
        console.log("Reversion Started");
        let scheduledProducts = scheduler[0].scheduledProducts;

        if (scheduledProducts?.length) {
            const uniqueProductIds = await uniqueByShopifyProductId(scheduledProducts);

            const productsWithTagsBefore = await Promise.all(scheduledProducts.map(async product => {
                let tagsBefore = [];
                try {
                    tagsBefore = Array.isArray(product.tags)
                        ? product.tags
                        : JSON.parse(product.tags_before || '[]');
                    if (!Array.isArray(tagsBefore)) throw new Error();
                } catch (e) {
                    console.error('Error parsing tags_before:', e.message);
                    tagsBefore = [];
                }
                return { id: product.shopifyProductId, tagsBefore };
            }));

            const productTagsBeforeMap = new Map(productsWithTagsBefore.map(item => [item.id, item.tagsBefore]));
            await retryWithBatchReduction(async (batchSize, resumeFromId) => {
                {
                    console.log("Resume tag", resumeFromId);
                    let batchCount = 0; // batch count
                    let startIndex = resumeFromId
                        ? uniqueProductIds.findIndex(p => p.shopifyProductId === resumeFromId)
                        : 0;
                    console.log(`Batch Size for tags: ${batchSize}`); // batch size
                    console.log(`Start Index for tags: ${startIndex}`); // safe index
                    console.log("Safe start index", startIndex == 0 ? 0 : Math.max(0, startIndex - batchSize))
                    startIndex = startIndex == 0 ? 0 : Math.max(0, startIndex - batchSize);
                    let lastProcessedId = resumeFromId;
                    let totalBatches = 0; // total bacthes
                    let totalVariantsMutated = 0; // total variants mutated
                    let successCount = 0; // Track successful updates
                    let failureCount = 0; // Track failed updates
                    for (let i = startIndex; i < uniqueProductIds.length; i += batchSize) {
                        const batch = uniqueProductIds.slice(i, i + batchSize);
                        batchCount++;
                        totalBatches++; 
                        totalVariantsMutated += batch.length; 
                        console.log(`Processing Batch for tags ${batchCount}`)
                        await Promise.all(batch.map(async ({ shopifyProductId }) => {
                            try {
                                let currentTags = await fetchCurrentTags(session, shopifyProductId);
                                let tagsToAdd = [];

                                const tagsBefore = productTagsBeforeMap.get(shopifyProductId) || [];
                                tagsToAdd = scheduler[0].selectedRemovedTags.filter(tag => tagsBefore.includes(tag));

                                const updatedTags = Array.from(new Set([...currentTags, ...tagsToAdd].filter(tag => !scheduler[0].selectedTags.includes(tag))));
                                //console.log(`Product ${shopifyProductId} - Adding Tags:`, updatedTags);
                                if (updatedTags.length) {
                                    lastProcessedId = shopifyProductId;
                                    successCount++;
                                    const res = await updateTags(session, shopifyProductId, updatedTags);
                                    //console.log("res", res)
                                }
                            }
                            catch (error) {
                                failureCount++;
                                //console.error('Error details:', error);
                                let errorWithId = { ...error, shopifyProductId };
                                //console.error('Error with product ID:', errorWithId);
                                throw errorWithId;
                            }
                        }));
                        await delay(700); // Delay between batches
                    }
                    console.log(`Total Batches Processed for Tags in this attempt: ${totalBatches}`);
                    console.log(`Total Prodiucts (tags) Mutated in this attempt: ${totalVariantsMutated}`); 
                    console.log(`Tags Update Success Count: ${successCount}`);
                    console.log(`Tags Update Failure Count: ${failureCount}`);
                    return lastProcessedId;
                }
            });


            // Batch process to update variant prices with retry mechanism
            await retryWithBatchReduction(async (batchSize, resumeFromId) => {
                await updateVariantPrice(session, scheduledProducts, 'revert', batchSize, resumeFromId);
            });


            await retryWithBatchReduction(async (batchSize, resumeFromId) => {
                await updateTagsForProducts(session, uniqueProductIds, scheduler[0].selectedTags, 'add', batchSize, resumeFromId); // Retain tags for traceability
            });
        }

        if (scheduler[0].chooseTheRollbackTheme && scheduler[0].revertToTheSpecificThemeLater) {
            await publishAndRevertTheme(session, scheduler[0].chooseTheRollbackTheme);
        }

        await updateSchedulerStatus(scheduler_id, { scheduler_status: 'completed' });
        errorOccured = false; // Ensure error is reset on success
        console.log("Reversion Completed");
    } catch (error) {
        console.error("Error in runRevertSchedulerJob:", error);
        errorOccured = true;
        await updateSchedulerStatus(scheduler_id, { scheduler_status: 'Pending Retry' });
        throw error;
    }

    // Only send email if no error occurred and not in retry status
    if (!errorOccured && scheduler[0].scheduler_status !== 'Pending Retry') {
        const testEmailData = {
            shopEmailAddress: "expertifies@gmail.com",
            jobName: scheduler[0].jobName || "Job Name",
            jobOperation: 'Reverted',
            scheduler: scheduler[0],
        };
        await sendEmailNotification(testEmailData);
    }

    return scheduler;
}


async function updateTagsForProducts(session, uniqueProductIds, tags, action, batchSize = 8, resumeFromId = null) {
    console.log("Resume tag", resumeFromId);
    let batchCount = 0; 
    // Find start index based on resumeFromId
    let startIndex = resumeFromId
        ? uniqueProductIds.findIndex(p => p.shopifyProductId === resumeFromId)
        : 0;
    console.log(`Batch Size for tags: ${batchSize}`); 
    console.log(`Start Index for tags: ${startIndex}`); 
    console.log("Safe start index", startIndex == 0 ? 0 : Math.max(0, startIndex - batchSize))
    startIndex = startIndex == 0 ? 0 : Math.max(0, startIndex - batchSize);
    let lastProcessedId = resumeFromId;
    let totalBatches = 0; 
    let totalVariantsMutated = 0; 
    let successCount = 0; 
    let failureCount = 0; 
    for (let i = startIndex; i < uniqueProductIds.length; i += batchSize) {
        const batch = uniqueProductIds.slice(i, i + batchSize);
        batchCount++;
        totalBatches++; 
        totalVariantsMutated += batch.length; 
        console.log(`Processing Batch for tags ${action} : ${batchCount}`); 
        await Promise.all(batch.map(async ({ shopifyProductId }) => {
            try {
                let currentTags = await fetchCurrentTags(session, shopifyProductId);
                let updatedTags;

                if (action === 'add') {
                    updatedTags = Array.from(new Set([...currentTags, ...tags]));
                } else {
                    updatedTags = currentTags.filter(tag => !tags.includes(tag));
                }

                //console.log(`Product ${shopifyProductId} - ${action === 'add' ? 'Adding' : 'Removing'} Tags:`, updatedTags);

                if (updatedTags.length) {
                    const updatedTagsResults = await updateTags(session, shopifyProductId, updatedTags);
                    lastProcessedId = shopifyProductId;
                    successCount++;
                    return lastProcessedId;
                }
            } catch (error) {
                //console.error(`Failed to update tags for product ${shopifyProductId}:`, error.message);
                failureCount++;
                let errorWithId = { ...error, shopifyProductId };
                throw errorWithId;
            }
        }));
        //lastProcessedId = batch[batch.length - 1]?.shopifyProductId;
    }
    console.log(`Total Batches Processed for Tags in this attempt: ${totalBatches}`);
    console.log(`Total Prodiucts (tags) Mutated in this attempt: ${totalVariantsMutated}`); // modified
    console.log(`Tags Update Success Count: ${successCount}`);
    console.log(`Tags Update Failure Count: ${failureCount}`);
    return lastProcessedId;
}

async function updateVariantPrice(session, products, action, batchSize = 8, resumeFromId = null) {
    console.log("Resume variant", resumeFromId);
    let startIndex = resumeFromId
        ? products.findIndex(p => p.id === resumeFromId)
        : 0;
    console.log(`Start Index for variants: ${startIndex}`);
    console.log("Safe start index for variants", Math.max(0, startIndex - batchSize))
    console.log(`Batch Size: ${batchSize}`); 
    let batchCount = 0; 
    let totalBatches = 0; 
    let totalVariantsMutated = 0; 
    let successCount = 0; 
    let failureCount = 0; 
    let lastProcessedId = resumeFromId;
    startIndex = Math.max(0, startIndex - 8);
    for (let i = startIndex; i < products.length; i += batchSize) {
        const batch = products.slice(i, i + batchSize);
        batchCount++;
        totalBatches++; 
        totalVariantsMutated += batch.length;
        console.log(`Processing Batch for variants ${action} : ${batchCount}`); 
        await Promise.all(batch.map(async product => {
            try {
                let priceData = {
                    id: product.id,
                    price: action === 'update' ? (product.modifiedPrice || 0) : (product.price || 0),
                    compareAtPrice: action === 'update' ? (product.modifiedCompareAtPrice || null) : (product.compareAtPrice || null)
                };

                // console.log(`Product ${product.id} - ${action === 'update' ? 'Updating' : 'Reverting'} Prices:`, priceData);
                const variantsResponse = await updateVariantPrices(session, priceData);
                successCount++;
                return variantsResponse;
            }
            catch (error) {
                console.error(`Failed to update variants for product ${product.id}:`, error.message);
                failureCount++;
                let errorWithId = { ...error, shopifyProductId: product.id }; //varirant id just named it as productid
                throw errorWithId;
            }
        }));
    }
    console.log(`Total Batches Processed for Variants in this attempt: ${totalBatches}`); 
    console.log(`Total Variants  Mutated in this attempt: ${totalVariantsMutated}`); 
    console.log(`Variants Update Success Count: ${successCount}`);
    console.log(`Variants Update Failure Count: ${failureCount}`);
    return lastProcessedId;
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
            console.error('GraphQL Errors:', data.errors);
            throw new Error(JSON.stringify(data.errors));
        } else if (data.data.productVariantUpdate.userErrors.length > 0) {
            console.error('User Errors:', data.data.productVariantUpdate.userErrors);
            throw new Error(JSON.stringify(data.data.productVariantUpdate.userErrors));
        } //else {
        //  console.log('Updated variant:', data.data.productVariantUpdate.productVariant);
        // }
    } catch (error) {
        console.error('Request failed:', error.message);
        throw error;
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
    const { shop, accessToken } = session;
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
    const { shop, accessToken } = session;
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

    try {
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
            throw new Error('Error updating tags: ' + JSON.stringify(response.data.errors));
        }

        if (response.data.data.productUpdate.userErrors.length > 0) {
            throw new Error('User Errors: ' + JSON.stringify(response.data.data.productUpdate.userErrors));
        }

        return response.data.data.productUpdate.product.tags;
    } catch (error) {
        console.error('Request failed:', error.message);
        throw error;
    }
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

// Function to get scheduler status so a i job wont run twice
async function getSchedulerJobStatus(session, scheduler_id) {
    const { shop } = session;
    try {
        let sql = `SELECT scheduler_status FROM schedulers WHERE id = ${scheduler_id} AND shop_name = '${shop}'`;
        const [rows] = await pool.query(sql);

        if (rows.length > 0) {
            return rows[0].scheduler_status;
        } else {
            throw new Error('Scheduler not found.');
        }
    } catch (err) {
        console.error('Error fetching job status:', err);
        throw err;
    }
}


/*
async function updateSchedularStatus(session, scheduler_id, status) {
    const { shop, accessToken } = session;
    try {
        let sql = `UPDATE schedulers SET scheduler_status = '${status}' WHERE id = ${scheduler_id} and shop_name = '${shop}' `;
        await pool.query(sql);
    } catch (err) {
        console.error('Error inserting scheduler status, transaction rolled back:', err);
        throw err;
    }
}
*/
// to update status when job runs or rollbacl (used app.jsx in such a way before calling the runschedulerjob function it add "inProgress" in scheduler_status after finishing the job it add "scheduledCompleted" similarly for rollback it add "reversionInprogress" and "completed") also need to update scheduleDate and time and vice versa for revertDate and time.
async function updateSchedularStatus(session, scheduler_id, status, DateToChange = null, datePayLoad = null, TimePayLoad = null) {
    const { shop } = session;

    // Defensive checks
    if (!scheduler_id) {
        console.error('Error: Scheduler ID is missing.');
        return { status: 301, message: "Scheduler ID is missing." };
    }

    if (!shop) {
        console.error('Error: Shop name is missing in session.');
        return { status: 301, message: "Shop name is missing in session." };
    }


    try {
        const connection = await pool.getConnection();

        try {
            await connection.beginTransaction();

            // Build the SQL query
            let sql = `
                UPDATE schedulers
                SET scheduler_status = '${status}'
            `;

            //  add the DateToChange update if provided
            if (DateToChange && datePayLoad && TimePayLoad) {
                sql += `,
                    ${DateToChange} = '${formatDateForMySQL(datePayLoad)} ${formatTimeForMySQL(TimePayLoad)}'
                `;
            }

            // Finalize the query with the WHERE clause
            sql += `
                WHERE id = ${scheduler_id} AND shop_name = '${shop}'
            `;

            await connection.query(sql);

            await connection.commit();
            console.log('Scheduler status updated successfully');
            return { status: 200, message: 'Scheduler status updated successfully' };
        } catch (err) {
            await connection.rollback();
            console.error('Error updating scheduler status, transaction rolled back:', err);
            return { status: 500, message: 'Error updating scheduler status' };
        } finally {
            connection.release();
        }
    } catch (err) {
        console.error('Error:', err);
        return { status: 500, message: 'Internal server error' };
    }
}



// to update job if user want to add roll back date time later (only allowed to happens if the jobstatus === "jobScheduled")
async function updateSchedulerDetails(session, scheduler_id, payload) {
    const { shop } = session;
    const {
        revertDate,
        revertTime,
        chooseTheRollbackTheme
    } = payload;

    // Defensive checks
    if (!scheduler_id) {
        console.error('Error: Scheduler ID is missing.');
        return { status: 301, message: "Scheduler ID is missing." };
    }

    if (!shop) {
        console.error('Error: Shop name is missing in session.');
        return { status: 301, message: "Shop name is missing in session." };
    }

    try {

        const connection = await pool.getConnection();

        try {
            await connection.beginTransaction();

            // Construct SQL query to update the specified columns
            let sql = `
                UPDATE schedulers
                SET revert_to_original_price_datetime = ${revertDate && revertTime
                    ? `'${formatDateForMySQL(revertDate)} ${formatTimeForMySQL(revertTime)}'`
                    : 'NULL'},
                    revert_to_original_price = 1,  
                    revert_theme_id = ${chooseTheRollbackTheme ? `'${chooseTheRollbackTheme}'` : 'NULL'}
                WHERE id = ${scheduler_id} AND shop_name = '${shop}'
            `;

            await connection.query(sql);

            await connection.commit();
            console.log('Scheduler details updated successfully');
            return { status: 200, message: 'Scheduler details updated successfully' };
        } catch (err) {
            await connection.rollback();
            console.error('Error updating scheduler details, transaction rolled back:', err);
            return { status: 500, message: 'Error updating scheduler details' };
        } finally {
            connection.release();
        }
    } catch (err) {
        console.error('Error:', err);
        return { status: 500, message: 'Internal server error' };
    }
}


module.exports = {
    getSchedulerJobByID,
    runSchedulerJob,
    runRevertSchedulerJob
};
