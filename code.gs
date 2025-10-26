// Configuration
const CONFIG = {
  SPREADSHEET_ID: '1emroQzuXEcNURzbr8BVMxfaZGINkg_rdrmy5kJpWOds',  // Add this
  SHEET_NAME: 'ALL AXIS',
  MAX_RETRIES: 3,
  RETRY_DELAY_MS: 1000,
  LOG_SHEET_NAME: 'Execution Logs'
};

// Utility function to log execution details
function logExecution(status, message, rowsProcessed = 0) {
  try {
    const ss = SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
    let logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
    if (!logSheet) {
      logSheet = ss.insertSheet(CONFIG.LOG_SHEET_NAME);
      logSheet.getRange(1, 1, 1, 4).setValues([['Timestamp', 'Status', 'Message', 'Rows Processed']])
        .setFontWeight('bold');
    }
    logSheet.insertRowAfter(1);
    logSheet.getRange(2, 1, 1, 4).setValues([[new Date(), status, message, rowsProcessed]]);
    
    // Keep only last 100 logs
    const lastRow = logSheet.getLastRow();
    if (lastRow > 101) { // 1 header + 100 logs
      logSheet.deleteRows(102, lastRow - 101);
    }
  } catch (err) {
    console.error('Logging failed:', err);
  }
}

// Utility function to handle retries
function withRetry(fn) {
  for (let i = 0; i < CONFIG.MAX_RETRIES; i++) {
    try {
      return fn();
    } catch (err) {
      if (i === CONFIG.MAX_RETRIES - 1) throw err;
      Utilities.sleep(CONFIG.RETRY_DELAY_MS * (i + 1));
    }
  }
}

function doPost(e) {
  try {
    // Use specific spreadsheet ID instead of active spreadsheet
    const ss = SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
    let sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
    if (!sheet) {
      sheet = ss.insertSheet(CONFIG.SHEET_NAME);
    }

    // Validate POST body
    if (!e.postData || !e.postData.contents) {
      return ContentService.createTextOutput("❌ Error: No POST data received");
    }

    const data = JSON.parse(e.postData.contents);
    if (!Array.isArray(data) || data.length === 0) {
      return ContentService.createTextOutput("❌ Error: POST data is empty or invalid");
    }

    // Use fixed 5 temp columns with requested aliases
    const aliasTempHeaders = ["Pulsa", "Grace Date", "Dead Date", "Masa Aktif", "Status"];
    const headers = ["Add-On", "Nomor", ...aliasTempHeaders, "active"];

    // If sheet has no header row yet, set it. Otherwise, ensure header matches desired labels
    const existingHeaderRange = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), headers.length));
    const existingHeader = existingHeaderRange.getValues()[0] || [];

    let needWriteHeader = false;
    if (existingHeader.length !== headers.length) {
      needWriteHeader = true;
    } else {
      for (let i = 0; i < headers.length; i++) {
        if ((existingHeader[i] || "") !== headers[i]) { needWriteHeader = true; break; }
      }
    }
    if (needWriteHeader) sheet.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight("bold");

    // Validate and sanitize input data with max records limit
    const MAX_RECORDS = 50000; // Limit records per execution for safety
    if (data.length > MAX_RECORDS) {
      logExecution('WARNING', `Data exceeded ${MAX_RECORDS} records limit. Processing first ${MAX_RECORDS} only.`);
      data = data.slice(0, MAX_RECORDS);
    }

    // Build values with exactly 5 temp columns (aliases above)
    const values = data.map(r => {
      // Sanitize input
      const sanitizedTemp = (r.temp || '').toString().replace(/[^\w\s|.-]/g, '');
      const parts = sanitizedTemp ? sanitizedTemp.split("|") : [];
      while (parts.length < 5) parts.push("");
      
      return [
        (r.DB || "").toString().slice(0, 50), // Limit string lengths for safety
        (r.msisdn || "").toString().slice(0, 20),
        parts[0].slice(0, 50) || "",
        parts[1].slice(0, 50) || "",
        parts[2].slice(0, 50) || "",
        parts[3].slice(0, 50) || "",
        parts[4].slice(0, 50) || "",
        (r.active || "").toString().slice(0, 10)
      ];
    });

    // Update existing rows (match by DB + msisdn) or append if not found
    const startDataRow = 2; // data starts after header
    const lastRow = sheet.getLastRow();
    let existingMap = new Map(); // key -> sheetRowIndex
    
    // Read existing data in chunks to handle large sheets
    if (lastRow >= startDataRow) {
      const CHUNK_SIZE = 10000;
      for (let startRow = startDataRow; startRow <= lastRow; startRow += CHUNK_SIZE) {
        const endRow = Math.min(startRow + CHUNK_SIZE - 1, lastRow);
        const numRows = endRow - startRow + 1;
        
        withRetry(() => {
          const chunkRange = sheet.getRange(startRow, 1, numRows, headers.length);
          const chunkValues = chunkRange.getValues();
          
          for (let i = 0; i < chunkValues.length; i++) {
            const row = chunkValues[i];
            const key = ((row[0] || "") + "|" + (row[1] || "")).toString();
            existingMap.set(key, startRow + i);
          }
        });
      }
    }

    // Process updates and new records in batches
    const BATCH_SIZE = 1000;
    const toAppend = [];
    const updates = [];
    
    for (let i = 0; i < values.length; i++) {
      const row = values[i];
      const key = ((row[0] || "") + "|" + (row[1] || "")).toString();
      if (existingMap.has(key)) {
        updates.push({
          row: row,
          sheetRow: existingMap.get(key)
        });
      } else {
        toAppend.push(row);
      }
      
      // Process updates in batches
      if (updates.length >= BATCH_SIZE) {
        withRetry(() => {
          updates.forEach(update => {
            sheet.getRange(update.sheetRow, 1, 1, headers.length).setValues([update.row]);
          });
        });
        updates.length = 0; // Clear the batch
      }
    }
    
    // Process remaining updates
    if (updates.length > 0) {
      withRetry(() => {
        updates.forEach(update => {
          sheet.getRange(update.sheetRow, 1, 1, headers.length).setValues([update.row]);
        });
      });
    }

    // Append new records in batches
    if (toAppend.length > 0) {
      for (let i = 0; i < toAppend.length; i += BATCH_SIZE) {
        const batch = toAppend.slice(i, i + BATCH_SIZE);
        withRetry(() => {
          sheet.getRange(sheet.getLastRow() + 1, 1, batch.length, batch[0].length).setValues(batch);
        });
      }
    }

    const message = `✅ Process completed! Updated: ${values.length - toAppend.length}, Appended: ${toAppend.length}`;
    logExecution('SUCCESS', message, values.length);
    return ContentService.createTextOutput(message);
  } catch (err) {
    const errorMessage = `❌ Error: ${err.toString()}`;
    logExecution('ERROR', errorMessage);
    return ContentService.createTextOutput(errorMessage);
  }
}

/**
 * Creates a time-driven trigger to run every 2 hours
 * Call this function manually once to set up the trigger
 */
function createTrigger() {
  // Delete any existing triggers first
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => ScriptApp.deleteTrigger(trigger));
  
  // Create new trigger to run every 2 hours
  ScriptApp.newTrigger('doPost')
    .timeBased()
    .everyHours(2)
    .create();
}

/**
 * Cleans up old data to prevent sheet from growing too large
 * Keeps last 90 days of data
 */
function cleanupOldData() {
  try {
    const ss = SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
    const sheet = ss.getSheetByName(CONFIG.SHEET_NAME);
    if (!sheet) return;

    const lastRow = sheet.getLastRow();
    if (lastRow <= 2) return; // Nothing to clean if only header row exists

    const values = sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).getValues();
    const today = new Date();
    const ninetyDaysAgo = new Date(today.getTime() - (90 * 24 * 60 * 60 * 1000));
    
    // Find rows to delete (assuming date is in column 4 - adjust if needed)
    let deleteCount = 0;
    for (let i = values.length - 1; i >= 0; i--) {
      const rowDate = new Date(values[i][3]); // Adjust column index if date is in different column
      if (rowDate < ninetyDaysAgo) {
        deleteCount = i + 2; // +2 because we start from row 2 and want to include this row
        break;
      }
    }
    
    if (deleteCount > 1) {
      sheet.deleteRows(2, deleteCount - 1);
      logExecution('INFO', `Cleaned up ${deleteCount - 1} old records`);
    }
  } catch (err) {
    logExecution('ERROR', `Cleanup failed: ${err.toString()}`);
  }
}
