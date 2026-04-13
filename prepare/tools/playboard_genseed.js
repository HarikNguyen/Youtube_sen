/**
 * Scrapes YouTube channel data from a scrollable list and exports it to CSV.
 * - Target: div.root (Scrollable container)
 * - Row: tr.chart__row
 * - Data: Name (Text) and Channel ID (Href minus prefix)
 */
async function scrapeYoutubeChannelsToCSV() {
    // --- 1. CONFIGURATION ---
    const scrollContainer = document.querySelector('div.root');
    const rowSelector = 'tr.chart__row';
    const fileName = 'vietnam_channels.csv';
    const idPrefix = '/en/channel/';

    if (!scrollContainer) {
        console.error("Target container 'div.root' not found!");
        return;
    }

    console.log("Scrolling started. Please keep this tab active...");

    // --- 2. AUTO-SCROLL LOGIC ---
    let lastRowCount = 0;
    let retryCount = 0;
    const maxRetries = 5;

    while (retryCount < maxRetries) {
        // Scroll to the bottom of the container
        scrollContainer.scrollTo(0, scrollContainer.scrollHeight);
        
        // Wait for 2.5s for new data to fetch and render
        await new Promise(resolve => setTimeout(resolve, 2500));

        let currentRowCount = document.querySelectorAll(rowSelector).length;
        console.log(`Current row count: ${currentRowCount}`);

        if (currentRowCount === lastRowCount) {
            // If no new rows, try "jiggling" the scroll to trigger lazy loading
            scrollContainer.scrollBy(0, -150);
            await new Promise(resolve => setTimeout(resolve, 500));
            scrollContainer.scrollTo(0, scrollContainer.scrollHeight);
            
            retryCount++;
            console.warn(`⚠️ No new data. Retry ${retryCount}/${maxRetries}...`);
        } else {
            retryCount = 0; // Reset retries if new rows are found
        }
        
        lastRowCount = currentRowCount;
    }

    console.log("Scrolling complete. Extracting data...");

    // --- 3. DATA EXTRACTION ---
    const rows = document.querySelectorAll(rowSelector);
    const csvRows = [["category", "name", "channel_id"]]; // Header

    rows.forEach((row) => {
        const nameTd = row.querySelector('td.name');
        if (nameTd) {
            const anchor = nameTd.querySelector('a.name__label');
            if (anchor) {
                // Get Name
                const name = anchor.innerText.trim();
                
                // Get Channel ID by removing the prefix from href
                const href = anchor.getAttribute('href') || "";
                const channelId = href.startsWith(idPrefix) 
                    ? href.substring(idPrefix.length) 
                    : href;

                // Format: Category 'C', escaped Name, processed ID
                csvRows.push(["C", `"${name.replace(/"/g, '""')}"`, `"${channelId}"`]);
            }
        }
    });

    // --- 4. EXPORT TO CSV ---
    const csvContent = csvRows.map(row => row.join(",")).join("\n");
    // Using \ufeff (BOM) to ensure UTF-8 characters display correctly in Excel
    const blob = new Blob(["\ufeff" + csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    
    const downloadLink = document.createElement("a");
    downloadLink.href = url;
    downloadLink.download = fileName;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);

    console.log(`Success! Exported ${csvRows.length - 1} channels to ${fileName}`);
}

// Run the script
scrapeYoutubeChannelsToCSV();
