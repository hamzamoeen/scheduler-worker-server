const nodemailer = require('nodemailer');

const transporter = nodemailer.createTransport({
    service: 'Zoho',
    host: 'smtp.zoho.com',
    port: 465,
    secure: true,
    auth: {
        user: "developer2@dezital.com",
        pass: "cfh*gXk9"   // WILL BE ENV LATER
    },
});

const sendEmail = async (to, subject, text, attachments = [], inlineImages = []) => {
    console.log("attachments", attachments);
    console.log("inlineImages", inlineImages);
    
    // Convert FormData files to Buffers
    const validInlineImages = inlineImages
        .filter(image => image && image.content && image.cid)
        .map(image => ({
            filename: image.filename,
            content: Buffer.from(image.content),
            cid: image.cid,
            contentType: image.contentType || 'image/png'
        }));

    const mailOptions = {
        from: "developer2@dezital.com",
        to,
        subject,
        text,
        html: text.replace(/cid:(\w+)/g, (match, p1) => `<img src="cid:${p1}">`),
        attachments: [...validInlineImages, ...attachments] // Include attachments and inline images
    };

    try {
        await transporter.sendMail(mailOptions);
        return {
            status: 'success',
            code: 200,
            message: 'Email sent successfully',
        };
    } catch (error) {
        console.error('Error sending email:', error.message);
        return {
            status: 'error',
            code: 500,
            message: `Email sending failed: ${error.message}`,
        };
    }
};

////////////// Mail to user about job completion
const options = [
    { label: 'Change the price by certain percentage', value: 'priceChangePercentage' },
    { label: 'Change the price to certain amount', value: 'priceChangeAmount' },
    { label: 'Change the price by certain amount', value: 'priceChangeToAAmount' },
    { label: 'Change the price to current compare at price (price before sale)', value: 'priceChangeAmountCompareAt' },
    { label: 'Change the price by certain amount relative to the compare at price', value: 'priceChangeAmountRelative' },
    { label: 'Change the price by certain percentage relative to the compare at price', value: 'priceChangePercentageCompareAt' },
    { label: 'Change the price by certain percentage relative to cost per item', value: 'priceChangePercentageRelative' },
    { label: 'Change price to certain cost margin', value: 'priceChangeAmountMargin' },
    { label: 'Don\'t change the price', value: 'noChangePrice' },
];

const optionCompareAt = [
    { label: 'Change the compare at price to the current price (sale)', value: 'priceChangeSale' },
    { label: 'Change the compare at price to certain amount', value: 'comparePriceChangeAmount' },
    { label: 'Change the compare at price by certain amount', value: 'comparePriceChangeToAAmount' },
    { label: 'Change the compare at price by certain percentage', value: 'priceChangePercentage' },
    { label: 'Change the compare at price by certain amount relative to the actual price', value: 'priceChangeAmountRelative' },
    { label: 'Change the compare at price by certain percentage relative to the actual price', value: 'priceChangePercentageCompareAt' },
    { label: 'Remove the compare at price', value: 'removeCompareAt' },
    { label: 'Don\'t change the compare at price', value: 'noChangePrice' },
];

const getLabel = (value, options) => {
    const option = options.find(opt => opt.value === value);
    return option ? option.label : 'Unknown';
};

function extractShopName(domain) {
    // Check if the domain contains "myshopify.com"
    if (domain.includes(".myshopify.com")) {
        // Extract the part before ".myshopify.com"
        const match = domain.match(/([^.]+)\.myshopify\.com/);
        return match ? match[1] : null;
    } else {
        // For custom domains, extract the part before the first "."
        const match = domain.match(/^([^.]+)/);
        return match ? match[1] : null;
    }
}


const generateEmailTemplate = ({
    jobName = 'Job',
    jobOperation = 'Operation',
    scheduler = {},
}) => {
    const {
        scheduledProducts = [],
        changePriceType = 'N/A',
        price = 0,
        shopName = "Shop Name",
        changeCAPriceType = 'N/A',
        compareAt = 0,
        chooseTheTheme = 'None',
        themeChangeWhenThisPriceChangeIsTriggered = false,
        jobId = '',
    } = scheduler;

    let productsSummary = '';
    
    if (scheduledProducts.length > 0) {
        scheduledProducts.forEach(product => {
            productsSummary += `
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">${product.title || 'N/A'}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">${product.price}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">${product.modifiedPrice}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">${product.compareAtPrice}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #ddd;">${product.modifiedCompareAtPrice}</td>
                </tr>
            `;
        });
    } else {
        productsSummary = `
            <tr>
                <td colspan="5" style="padding: 10px; text-align: center; color: #999;">
                    No products were scheduled for this job.
                </td>
            </tr>
        `;
    }

    return `
        <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #333;">
            <h2 style="background-color: #0073aa; color: #fff; padding: 10px;">Hey, ${extractShopName(shopName)}!</h2>
            ${
                scheduledProducts.length > 0 ?
                 `<p>Your <strong>${jobName}</strong> has successfully <strong>${jobOperation}</strong> and all the product variants affected by this job <strong>(${scheduledProducts.length || 0}</strong> total) have been updated.</p>` :
                    `<p>Your <strong>${jobName}</strong> has successfully <strong>${jobOperation}</strong> and Theme is changed</p>`
            }
            ${scheduledProducts.length > 0 ? `
            <h3 style="color: #0073aa;">Quick Review of Changes:</h3>
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                <tr>
                    <th style="padding: 10px; border-bottom: 2px solid #0073aa; text-align: left;">Product Variant</th>
                    <th style="padding: 10px; border-bottom: 2px solid #0073aa; text-align: left;">Old Price</th>
                    <th style="padding: 10px; border-bottom: 2px solid #0073aa; text-align: left;">New Price</th>
                    <th style="padding: 10px; border-bottom: 2px solid #0073aa; text-align: left;">Old Compare At Price</th>
                    <th style="padding: 10px; border-bottom: 2px solid #0073aa; text-align: left;">New Compare At Price</th>
                </tr>
                ${productsSummary}
            </table>
            ` : ""}

            <h4 style="color: #0073aa;">Original Request:</h4>
            ${scheduledProducts.length > 0 ? `
            <ul style="list-style-type: none; padding: 0;">
                <li>You selected <strong>${getLabel(changePriceType, options)}</strong> by (<strong>${price}%</strong>)</li>
                <li>Option selected: <strong>${getLabel(changeCAPriceType, optionCompareAt)}</strong> ${compareAt == 0 ? '' : `(<strong>${compareAt}</strong>)`}</li> 
                <li>Theme change requested: <strong>${themeChangeWhenThisPriceChangeIsTriggered ? `Completed` : "Not Requested"}</strong></li>
            </ul>
            ` : `
            <ul style="list-style-type: none; padding: 0;">
                <li>Theme change requested: <strong>${themeChangeWhenThisPriceChangeIsTriggered ? `Completed` : "Not Requested"}</strong></li>
            </ul>
            `}

            <p style="text-align: left; margin: 20px 0;">
                <a href="${shopName ? `https://admin.shopify.com/store/${shopName}/apps/price-theme-scheduler-dev/app/${jobId}` : '#'}" 
                   style="background-color: #0073aa; color: #fff; padding: 10px 20px; text-decoration: none; border-radius: 5px;">
                    View full price change details
                </a>
            </p>

            <p>If you have any questions or feedback, please contact our support team at <a href="mailto:support@dezital.com" style="color: #0073aa;">support@dezital.com</a>.</p>

            <p>Thank you for using Price & Theme Scheduler!</p>
        </div>
    `;
};



const sendEmailNotification = async (emailParams) => {
    const {
        shopEmailAddress,
        jobName,
        jobOperation,
        scheduler,
    } = emailParams;

    // Defensive checks
    if (!shopEmailAddress || !jobName || !jobOperation) {
        console.error('Missing required email parameters');
        return {
            status: 'error',
            code: 400,
            message: 'Missing required email parameters',
        };
    }

    // Generate email template using the provided parameters
    const emailTemplate = generateEmailTemplate({
        jobName,
        jobOperation,
        scheduler,
    });

    const mailOptions = {
        from: "developer2@dezital.com",
        to: shopEmailAddress,
        subject: `${jobName} ${jobOperation} - Price & Theme Scheduler`,
        html: emailTemplate,
    };

    try {
        await transporter.sendMail(mailOptions);
        return {
            status: 'success',
            code: 200,
            message: 'Email sent successfully',
        };
    } catch (error) {
        console.error('Error sending email:', error.message);
        return {
            status: 'error',
            code: 500,
            message: `Email sending failed: ${error.message}`,
        };
    }
};


module.exports = {
    sendEmailNotification
};
