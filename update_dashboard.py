import os
import re

dashboard_path = 'dashboard/src/pages/Dashboard.js'

with open(dashboard_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Update activePredictions sorting
content = content.replace('sort((left, right) => right.predicted_quantity - left.predicted_quantity)', 'sort((left, right) => right.predicted_waste - left.predicted_waste)')

# Update alerts
content = content.replace('row.is_high_demand_alert', 'row.is_high_waste_alert')

# Update metrics extraction
old_metrics_extract = '''  const alertCount = activePredictions.filter((row) => row.is_high_demand_alert).length;
  const totalPredicted = activePredictions.reduce((sum, row) => sum + (row.predicted_quantity || 0), 0);
  const liveSlot = liveData?.meal_slot || selectedSlot;
  const topPrediction = activePredictions[0];'''

new_metrics_extract = '''  const alertCount = activePredictions.filter((row) => row.is_high_waste_alert).length;
  const totalWaste = activePredictions.reduce((sum, row) => sum + (row.predicted_waste || 0), 0);
  const totalCostLost = activePredictions.reduce((sum, row) => sum + (row.cost_lost || 0), 0);
  const totalPrepared = activePredictions.reduce((sum, row) => sum + (row.predicted_preparation || 0), 0);
  const wastePercentage = totalPrepared ? ((totalWaste / totalPrepared) * 100).toFixed(1) : 0;
  const liveSlot = liveData?.meal_slot || selectedSlot;
  const topPrediction = activePredictions[0];'''

content = content.replace(old_metrics_extract, new_metrics_extract)

# Update metrics array
old_metrics_arr = '''    {
      label: "Predicted Portions",
      value: totalPredicted.toLocaleString(),
      hint: "Total quantity for the selected meal and day profile",
      accent: "amber",
      icon: <Rows3 size={18} />,
    },
    {
      label: "High-Demand Alerts",
      value: alertCount.toString(),
      hint: "Items crossing the demand alert threshold",
      accent: alertCount ? "rose" : "slate",
      icon: <Radar size={18} />,
    },'''

new_metrics_arr = '''    {
      label: "Total Predicted Waste",
      value: `${totalWaste.toLocaleString()} portions`,
      hint: `Est. ${wastePercentage}% of prepared food`,
      accent: "amber",
      icon: <Rows3 size={18} />,
    },
    {
      label: "High-Waste Alerts",
      value: alertCount.toString(),
      hint: "Items crossing the waste alert threshold (20%)",
      accent: alertCount ? "rose" : "slate",
      icon: <Radar size={18} />,
    },
    {
      label: "Cost Lost",
      value: `₹${totalCostLost.toLocaleString()}`,
      hint: "Est. monetary loss due to wasted food",
      accent: "rose",
      icon: <Radar size={18} />,
    },'''

content = content.replace(old_metrics_arr, new_metrics_arr)

# Remove spark records metric to make room (6 metrics layout usually)
content = re.sub(r'\{\s*label:\s*"Spark Records".*?\},', '', content, flags=re.DOTALL)

# Update top predicted item hint
content = content.replace('topPrediction ? `${topPrediction.predicted_quantity} portions` : "No active prediction payload yet"', 'topPrediction ? `${topPrediction.predicted_waste} wasted portions` : "No active prediction payload yet"')

# Update titles
content = content.replace('Smart Mess Food Demand Predictor', 'Smart Waste Management Intelligence')
content = content.replace('Campus Demand Intelligence', 'Campus Waste Intelligence')
content = content.replace('predicted portions for', 'portions of expected waste for')
content = content.replace('totalPredicted.toLocaleString()', 'totalWaste.toLocaleString()')
content = content.replace('High-Demand Alerts', 'High-Waste Alerts')
content = content.replace('Predicted Portions', 'Total Predicted Waste')

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(content)

# Update PredictionTable.jsx
table_path = 'dashboard/src/components/PredictionTable.jsx'
with open(table_path, 'r', encoding='utf-8') as f:
    table_content = f.read()

table_content = table_content.replace('<th>Forecast</th>', '<th>Consumption</th>')
table_content = table_content.replace('<th>Buffer</th>', '<th>Preparation</th>')
table_content = table_content.replace('<th>Confidence</th>', '<th>Est. Waste</th>')
table_content = table_content.replace('row.predicted_quantity', 'row.predicted_consumption')
table_content = table_content.replace('row.buffer_quantity', 'row.predicted_preparation')
table_content = table_content.replace('row.confidence', '`${row.predicted_waste} (${row.waste_percentage}%)`')
table_content = table_content.replace('row.is_high_demand_alert', 'row.is_high_waste_alert')

with open(table_path, 'w', encoding='utf-8') as f:
    f.write(table_content)

print("Updated dashboard frontend successfully")
