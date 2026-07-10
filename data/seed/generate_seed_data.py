"""
Generates 2-year historical mess demand data (~32,850 rows).
Run: python data/seed/generate_seed_data.py
"""
import csv, random, math, os
from datetime import date, timedelta

random.seed(42)
FOOD_ITEMS = ["idli","dosa","poha","upma","bread_butter","rice","chapati",
              "dal","sambar","rasam","veg_curry","egg_curry","curd_rice","pulao","biryani"]
MEAL_SLOTS = ["breakfast","lunch","dinner"]
BASE = {
    "breakfast":{"idli":280,"dosa":220,"poha":180,"upma":150,"bread_butter":200,
                 "rice":50,"chapati":60,"dal":40,"sambar":90,"rasam":50,
                 "veg_curry":70,"egg_curry":60,"curd_rice":80,"pulao":40,"biryani":30},
    "lunch":{"idli":30,"dosa":20,"poha":15,"upma":10,"bread_butter":20,
             "rice":520,"chapati":380,"dal":400,"sambar":460,"rasam":350,
             "veg_curry":390,"egg_curry":280,"curd_rice":310,"pulao":180,"biryani":150},
    "dinner":{"idli":50,"dosa":60,"poha":30,"upma":25,"bread_butter":40,
              "rice":450,"chapati":420,"dal":380,"sambar":300,"rasam":280,
              "veg_curry":360,"egg_curry":310,"curd_rice":200,"pulao":240,"biryani":280}
}
FIXED_HOL = {(1,1),(1,26),(8,15),(10,2),(12,25),(4,14),(3,25),(11,4)}
EVENT_POOL = (["none"]*20+["exam_week"]*4+["cultural_fest"]*2+
              ["sports_day"]*2+["holiday"]*3+["fresher_party"]*1+["convocation"]*1)
IMPACT = {"cultural_fest":1.30,"sports_day":1.20,"fresher_party":1.40,
          "convocation":1.25,"exam_week":0.95,"holiday":0.60,"none":1.00}

rows = []
start = date(2023,1,1)
for i in range(730):
    d = start + timedelta(days=i)
    hol  = int((d.month,d.day) in FIXED_HOL or d.weekday()>=6)
    exam = int((d.month==11 and d.day>=15) or (d.month==4 and 1<=d.day<=20))
    day_type = "holiday" if hol else ("weekend" if d.weekday()>=5 else "weekday")
    wtemp = round(25+10*math.sin(2*math.pi*(d.month-3)/12)+random.gauss(0,2),1)
    base_occ = 480
    if hol: base_occ=int(base_occ*0.60)
    elif d.weekday()>=5: base_occ=int(base_occ*0.85)
    if exam: base_occ=int(base_occ*0.95)
    h_count = min(500,max(80,base_occ+random.randint(-25,25)))
    event = random.choice(EVENT_POOL)
    ei = IMPACT.get(event,1.0)
    for slot in MEAL_SLOTS:
        for item in FOOD_ITEMS:
            b = BASE[slot][item]
            sea = 1+0.15*math.sin(2*math.pi*d.month/12)
            dow = 1.1 if d.weekday()==4 else (0.82 if d.weekday()>=5 else 1.0)
            hl  = 0.55 if hol else 1.0
            occ = h_count/500.0
            e2  = ei
            if item=="biryani" and (d.weekday()>=4 or event not in("none","exam_week")):
                e2=min(2.2,e2*1.45)
            # Simulate actual consumption
            qty = max(0,int(b*sea*dow*hl*occ*e2*random.uniform(0.88,1.12)))

            # Simulate Mess Manager preparation behavior:
            # They don't perfectly adjust for holidays or exams, often over-preparing
            manager_hl = 0.85 if hol else 1.0 # Only drops 15% instead of actual 45%
            manager_occ = min(1.0, (base_occ + 50) / 500.0) # Overestimates occupancy
            
            prep_qty = max(0,int(b*sea*dow*manager_hl*manager_occ*random.uniform(0.95,1.15)))
            # Ensure prep is at least slightly more than consumption most of the time
            prep_qty = max(prep_qty, qty + random.randint(0, int(qty*0.1)))
            waste = prep_qty - qty
            cost_per_portion = random.randint(15, 60)
            
            rows.append([d.isoformat(),slot,item,qty, prep_qty, waste, cost_per_portion, day_type,
                         h_count,wtemp,hol,exam,event,d.weekday(),d.month,d.isocalendar()[1]])

header=["date","meal_slot","food_item","quantity_consumed", "quantity_prepared", "waste_generated", "cost_per_portion", "day_type",
        "hostel_count","weather_temp","is_holiday","is_exam_week",
        "event_name","day_of_week","month","week_of_year"]
out=os.path.join(os.path.dirname(os.path.abspath(__file__)),"historical_mess_data.csv")
with open(out,"w",newline="") as f:
    w=csv.writer(f); w.writerow(header); w.writerows(rows)
print(f"Generated {len(rows):,} rows -> {out}")