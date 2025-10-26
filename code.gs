function doPost(e) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName("ALL AXIS");
    if (!sheet) {
      sheet = ss.insertSheet("ALL AXIS");
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

    // Build values with exactly 5 temp columns (aliases above)
    const values = data.map(r => {
      const parts = r.temp ? r.temp.toString().split("|") : [];
      // pad with empty strings to have exactly 5 parts
      while (parts.length < 5) parts.push("");
      return [
        r.DB || "",
        r.msisdn || "",
        parts[0] || "",
        parts[1] || "",
        parts[2] || "",
        parts[3] || "",
        parts[4] || "",
        r.active || ""
      ];
    });

    // Update existing rows (match by DB + msisdn) or append if not found
    const startDataRow = 2; // data starts after header
    const lastRow = sheet.getLastRow();
    let existingMap = new Map(); // key -> sheetRowIndex
    if (lastRow >= startDataRow) {
      const existingRange = sheet.getRange(startDataRow, 1, lastRow - startDataRow + 1, headers.length);
      const existingValues = existingRange.getValues();
      for (let i = 0; i < existingValues.length; i++) {
        const row = existingValues[i];
        const key = ((row[0] || "") + "|" + (row[1] || "")).toString();
        existingMap.set(key, startDataRow + i);
      }
    }

    const toAppend = [];
    for (let i = 0; i < values.length; i++) {
      const row = values[i];
      const key = ((row[0] || "") + "|" + (row[1] || "")).toString();
      if (existingMap.has(key)) {
        const sheetRow = existingMap.get(key);
        // update that row (overwrite all columns for simplicity)
        sheet.getRange(sheetRow, 1, 1, headers.length).setValues([row]);
      } else {
        toAppend.push(row);
      }
    }

    if (toAppend.length > 0) {
      sheet.getRange(sheet.getLastRow() + 1, 1, toAppend.length, toAppend[0].length).setValues(toAppend);
    }

    return ContentService.createTextOutput(`✅ Data appended! Rows: ${values.length}`);
  } catch (err) {
    return ContentService.createTextOutput("❌ Error: " + err);
  }
}
