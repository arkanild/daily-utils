function splitResults() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Sheet1"); // Change to your sheet name
  const dataRange = sheet.getRange(2, 4, sheet.getLastRow() - 1, 1); // Adjust column (4 = D) as needed
  const data = dataRange.getValues();
  
  for (let i = data.length - 1; i >= 0; i--) {
    const row = data[i][0];
    if (row && row.includes(",")) {
      const splitValues = row.split(", ");
      sheet.deleteRow(i + 2); // Remove the original combined row
      splitValues.forEach((val, idx) => {
        sheet.insertRowBefore(i + 2 + idx);
        sheet.getRange(i + 2 + idx, 4).setValue(val); // Adjust column as needed
      });
    }
  }
}
