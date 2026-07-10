import re

with open('api/main.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace predict_demand with predict_waste
old_predict_demand = r'''# ── Prediction engine ─────────────────────────────────────────────────
def predict_demand\(food_item: str, meal_slot: str, day_type: str = "weekday"\) -> dict:.*?    \}'''

new_predict_waste = '''# ── Prediction engine ─────────────────────────────────────────────────
def predict_waste(food_item: str, meal_slot: str, day_type: str = "weekday") -> dict:
    df = read_demand_df()
    base_demand = 60
    base_prep = 80
    confidence  = 0.65
    data_source = "rule-based"

    if not df.empty and "food_item" in df.columns:
        mask    = (df["food_item"] == food_item) & (df["meal_slot"] == meal_slot)
        subset  = df[mask]
        if not subset.empty:
            base_demand = int(subset["total_quantity"].sum())
            if "total_prepared" in subset.columns:
                 base_prep = int(subset["total_prepared"].sum())
            else:
                 base_prep = int(base_demand * 1.25)
            confidence  = min(0.97, 0.70 + len(subset) * 0.05)
            data_source = "spark-hdfs"

    day_mult  = {"weekday": 1.0, "weekend": 0.78, "holiday": 0.45}.get(day_type, 1.0)
    slot_mult = {"breakfast": 0.85, "lunch": 1.25, "dinner": 1.0}.get(meal_slot, 1.0)
    
    predicted_consumption = max(10, int(base_demand * day_mult * slot_mult))
    # Mess managers tend to not reduce prep enough on holidays
    prep_mult = {"weekday": 1.0, "weekend": 0.90, "holiday": 0.80}.get(day_type, 1.0)
    predicted_prep = max(15, int(base_prep * prep_mult * slot_mult))
    
    # Ensure prep is always at least consumption
    predicted_prep = max(predicted_prep, predicted_consumption + 5)
    
    predicted_waste = predicted_prep - predicted_consumption
    waste_percentage = round((predicted_waste / predicted_prep) * 100, 1)
    
    is_high_waste_alert = waste_percentage > 20
    
    cost_per_portion = 35 # Assuming avg 35 rupees per portion
    cost_lost = predicted_waste * cost_per_portion
    
    recommendation = ""
    if is_high_waste_alert:
        recommended_reduction = int(predicted_waste * 0.8)
        recommendation = f"⚠️ HIGH WASTE PREDICTED. Reduce preparation by {recommended_reduction} portions to save ₹{recommended_reduction * cost_per_portion}!"
    else:
        recommendation = "Waste levels are within normal bounds. Keep current preparation."

    return {
        "food_item":            food_item,
        "meal_slot":            meal_slot,
        "day_type":             day_type,
        "predicted_consumption":predicted_consumption,
        "predicted_preparation":predicted_prep,
        "predicted_waste":      predicted_waste,
        "waste_percentage":     waste_percentage,
        "cost_lost":            cost_lost,
        "confidence":           round(confidence, 2),
        "data_source":          data_source,
        "is_high_waste_alert":  is_high_waste_alert,
        "recommendation":       recommendation,
        "timestamp": datetime.now().isoformat(),
    }'''

content = re.sub(old_predict_demand, new_predict_waste, content, flags=re.DOTALL)

# Routes replacements
content = content.replace('@app.post("/predict/demand")', '@app.post("/predict/waste")')
content = content.replace('def predict_single(req: PredictRequest):\n    return predict_demand(req.food_item, req.meal_slot, req.day_type)', 'def predict_single(req: PredictRequest):\n    return predict_waste(req.food_item, req.meal_slot, req.day_type)')

old_predict_all = '''@app.get("/predict/all/{meal_slot}")
def predict_all(meal_slot: str, day_type: str = "weekday"):
    results = sorted(
        [predict_demand(item, meal_slot, day_type) for item in FOOD_ITEMS],
        key=lambda x: x["predicted_quantity"], reverse=True
    )
    alerts = [r["food_item"] for r in results if r["is_high_demand_alert"]]
    return {
        "meal_slot":                meal_slot,
        "day_type":                 day_type,
        "predictions":              results,
        "total_predicted_portions": sum(r["predicted_quantity"] for r in results),
        "high_demand_alerts":       len(alerts),
        "alert_items":              alerts,
        "generated_at":             datetime.now().isoformat(),
    }'''

new_predict_all = '''@app.get("/predict/all/{meal_slot}")
def predict_all(meal_slot: str, day_type: str = "weekday"):
    results = sorted(
        [predict_waste(item, meal_slot, day_type) for item in FOOD_ITEMS],
        key=lambda x: x["predicted_waste"], reverse=True
    )
    alerts = [r["food_item"] for r in results if r["is_high_waste_alert"]]
    return {
        "meal_slot":                meal_slot,
        "day_type":                 day_type,
        "predictions":              results,
        "total_predicted_waste":    sum(r["predicted_waste"] for r in results),
        "total_cost_lost":          sum(r["cost_lost"] for r in results),
        "high_waste_alerts":        len(alerts),
        "alert_items":              alerts,
        "generated_at":             datetime.now().isoformat(),
    }'''

content = content.replace(old_predict_all, new_predict_all)

# Websocket replacements
content = content.replace('"predictions":    [predict_demand(f, meal_slot) for f in FOOD_ITEMS]', '"predictions":    [predict_waste(f, meal_slot) for f in FOOD_ITEMS]')

with open('api/main.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated api/main.py successfully")
