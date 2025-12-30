import base64
import json
import csv
import io

FIELD_ORDER = [
    "country", "mkt_name", "dates", "year", "month",
    "o_apples", "h_apples", "l_apples", "c_apples", "inflation_apples", "trust_apples",
    "o_bananas", "h_bananas", "l_bananas", "c_bananas", "inflation_bananas", "trust_bananas",
    "o_beans", "h_beans", "l_beans", "c_beans", "inflation_beans", "trust_beans",
    "o_bread", "h_bread", "l_bread", "c_bread", "inflation_bread", "trust_bread",
    "o_bulgur", "h_bulgur", "l_bulgur", "c_bulgur", "inflation_bulgur", "trust_bulgur",
    "o_cabbage", "h_cabbage", "l_cabbage", "c_cabbage", "inflation_cabbage", "trust_cabbage",
    "o_carrots", "h_carrots", "l_carrots", "c_carrots", "inflation_carrots", "trust_carrots",
    "o_cassava", "h_cassava", "l_cassava", "c_cassava", "inflation_cassava", "trust_cassava",
    "o_cassava_flour", "h_cassava_flour", "l_cassava_flour", "c_cassava_flour", "inflation_cassava_flour", "trust_cassava_flour",
    "o_cassava_meal", "h_cassava_meal", "l_cassava_meal", "c_cassava_meal", "inflation_cassava_meal", "trust_cassava_meal",
    "o_cheese", "h_cheese", "l_cheese", "c_cheese", "inflation_cheese", "trust_cheese",
    "o_chickpeas", "h_chickpeas", "l_chickpeas", "c_chickpeas", "inflation_chickpeas", "trust_chickpeas",
    "o_chili", "h_chili", "l_chili", "c_chili", "inflation_chili", "trust_chili",
    "o_coffee_instant", "h_coffee_instant", "l_coffee_instant", "c_coffee_instant", "inflation_coffee_instant", "trust_coffee_instant",
    "o_couscous", "h_couscous", "l_couscous", "c_couscous", "inflation_couscous", "trust_couscous",
    "o_cowpeas", "h_cowpeas", "l_cowpeas", "c_cowpeas", "inflation_cowpeas", "trust_cowpeas",
    "o_cucumbers", "h_cucumbers", "l_cucumbers", "c_cucumbers", "inflation_cucumbers", "trust_cucumbers",
    "o_dates", "h_dates", "l_dates", "c_dates", "inflation_dates", "trust_dates",
    "o_eggplants", "h_eggplants", "l_eggplants", "c_eggplants", "inflation_eggplants", "trust_eggplants",
    "o_eggs", "h_eggs", "l_eggs", "c_eggs", "inflation_eggs", "trust_eggs",
    "o_fish", "h_fish", "l_fish", "c_fish", "inflation_fish", "trust_fish",
    "o_fish_catfish", "h_fish_catfish", "l_fish_catfish", "c_fish_catfish", "inflation_fish_catfish", "trust_fish_catfish",
    "o_fish_mackerel", "h_fish_mackerel", "l_fish_mackerel", "c_fish_mackerel", "inflation_fish_mackerel", "trust_fish_mackerel",
    "o_fish_salted", "h_fish_salted", "l_fish_salted", "c_fish_salted", "inflation_fish_salted", "trust_fish_salted",
    "o_fish_sardine_canned", "h_fish_sardine_canned", "l_fish_sardine_canned", "c_fish_sardine_canned", "inflation_fish_sardine_canned", "trust_fish_sardine_canned",
    "o_fish_smoked", "h_fish_smoked", "l_fish_smoked", "c_fish_smoked", "inflation_fish_smoked", "trust_fish_smoked",
    "o_fish_tilapia", "h_fish_tilapia", "l_fish_tilapia", "c_fish_tilapia", "inflation_fish_tilapia", "trust_fish_tilapia",
    "o_fish_tuna_canned", "h_fish_tuna_canned", "l_fish_tuna_canned", "c_fish_tuna_canned", "inflation_fish_tuna_canned", "trust_fish_tuna_canned",
    "o_gari", "h_gari", "l_gari", "c_gari", "inflation_gari", "trust_gari",
    "o_garlic", "h_garlic", "l_garlic", "c_garlic", "inflation_garlic", "trust_garlic",
    "o_groundnuts", "h_groundnuts", "l_groundnuts", "c_groundnuts", "inflation_groundnuts", "trust_groundnuts",
    "o_groundnuts_paste", "h_groundnuts_paste", "l_groundnuts_paste", "c_groundnuts_paste", "inflation_groundnuts_paste", "trust_groundnuts_paste",
    "o_lentils", "h_lentils", "l_lentils", "c_lentils", "inflation_lentils", "trust_lentils",
    "o_lettuce", "h_lettuce", "l_lettuce", "c_lettuce", "inflation_lettuce", "trust_lettuce",
    "o_livestock_sheep_two_year_old_male", "h_livestock_sheep_two_year_old_male", "l_livestock_sheep_two_year_old_male", "c_livestock_sheep_two_year_old_male", "inflation_livestock_sheep_two_year_old_male", "trust_livestock_sheep_two_year_old_male",
    "o_livestocksheep_castrated_male", "h_livestocksheep_castrated_male", "l_livestocksheep_castrated_male", "c_livestocksheep_castrated_male", "inflation_livestocksheep_castrated_male", "trust_livestocksheep_castrated_male",
    "o_maize", "h_maize", "l_maize", "c_maize", "inflation_maize", "trust_maize",
    "o_maize_flour", "h_maize_flour", "l_maize_flour", "c_maize_flour", "inflation_maize_flour", "trust_maize_flour",
    "o_maize_meal", "h_maize_meal", "l_maize_meal", "c_maize_meal", "inflation_maize_meal", "trust_maize_meal",
    "o_meat_beef", "h_meat_beef", "l_meat_beef", "c_meat_beef", "inflation_meat_beef", "trust_meat_beef",
    "o_meat_beef_canned", "h_meat_beef_canned", "l_meat_beef_canned", "c_meat_beef_canned", "inflation_meat_beef_canned", "trust_meat_beef_canned",
    "o_meat_beef_chops", "h_meat_beef_chops", "l_meat_beef_chops", "c_meat_beef_chops", "inflation_meat_beef_chops", "trust_meat_beef_chops",
    "o_meat_beef_minced", "h_meat_beef_minced", "l_meat_beef_minced", "c_meat_beef_minced", "inflation_meat_beef_minced", "trust_meat_beef_minced",
    "o_meat_buffalo", "h_meat_buffalo", "l_meat_buffalo", "c_meat_buffalo", "inflation_meat_buffalo", "trust_meat_buffalo",
    "o_meat_chicken", "h_meat_chicken", "l_meat_chicken", "c_meat_chicken", "inflation_meat_chicken", "trust_meat_chicken",
    "o_meat_chicken_broiler", "h_meat_chicken_broiler", "l_meat_chicken_broiler", "c_meat_chicken_broiler", "inflation_meat_chicken_broiler", "trust_meat_chicken_broiler",
    "o_meat_chicken_frozen", "h_meat_chicken_frozen", "l_meat_chicken_frozen", "c_meat_chicken_frozen", "inflation_meat_chicken_frozen", "trust_meat_chicken_frozen",
    "o_meat_chicken_plucked", "h_meat_chicken_plucked", "l_meat_chicken_plucked", "c_meat_chicken_plucked", "inflation_meat_chicken_plucked", "trust_meat_chicken_plucked",
    "o_meat_chicken_whole", "h_meat_chicken_whole", "l_meat_chicken_whole", "c_meat_chicken_whole", "inflation_meat_chicken_whole", "trust_meat_chicken_whole",
    "o_meat_chicken_whole_frozen", "h_meat_chicken_whole_frozen", "l_meat_chicken_whole_frozen", "c_meat_chicken_whole_frozen", "inflation_meat_chicken_whole_frozen", "trust_meat_chicken_whole_frozen",
    "o_meat_goat", "h_meat_goat", "l_meat_goat", "c_meat_goat", "inflation_meat_goat", "trust_meat_goat",
    "o_meat_lamb", "h_meat_lamb", "l_meat_lamb", "c_meat_lamb", "inflation_meat_lamb", "trust_meat_lamb",
    "o_meat_pork", "h_meat_pork", "l_meat_pork", "c_meat_pork", "inflation_meat_pork", "trust_meat_pork",
    "o_milk", "h_milk", "l_milk", "c_milk", "inflation_milk", "trust_milk",
    "o_millet", "h_millet", "l_millet", "c_millet", "inflation_millet", "trust_millet",
    "o_oil", "h_oil", "l_oil", "c_oil", "inflation_oil", "trust_oil",
    "o_onions", "h_onions", "l_onions", "c_onions", "inflation_onions", "trust_onions",
    "o_oranges", "h_oranges", "l_oranges", "c_oranges", "inflation_oranges", "trust_oranges",
    "o_parsley", "h_parsley", "l_parsley", "c_parsley", "inflation_parsley", "trust_parsley",
    "o_pasta", "h_pasta", "l_pasta", "c_pasta", "inflation_pasta", "trust_pasta",
    "o_peas", "h_peas", "l_peas", "c_peas", "inflation_peas", "trust_peas",
    "o_plantains", "h_plantains", "l_plantains", "c_plantains", "inflation_plantains", "trust_plantains",
    "o_potatoes", "h_potatoes", "l_potatoes", "c_potatoes", "inflation_potatoes", "trust_potatoes",
    "o_pulses", "h_pulses", "l_pulses", "c_pulses", "inflation_pulses", "trust_pulses",
    "o_rice", "h_rice", "l_rice", "c_rice", "inflation_rice", "trust_rice",
    "o_rice_various", "h_rice_various", "l_rice_various", "c_rice_various", "inflation_rice_various", "trust_rice_various",
    "o_salt", "h_salt", "l_salt", "c_salt", "inflation_salt", "trust_salt",
    "o_sesame", "h_sesame", "l_sesame", "c_sesame", "inflation_sesame", "trust_sesame",
    "o_sorghum", "h_sorghum", "l_sorghum", "c_sorghum", "inflation_sorghum", "trust_sorghum",
    "o_sorghum_food_aid", "h_sorghum_food_aid", "l_sorghum_food_aid", "c_sorghum_food_aid", "inflation_sorghum_food_aid", "trust_sorghum_food_aid",
    "o_sugar", "h_sugar", "l_sugar", "c_sugar", "inflation_sugar", "trust_sugar",
    "o_tea", "h_tea", "l_tea", "c_tea", "inflation_tea", "trust_tea",
    "o_tomatoes", "h_tomatoes", "l_tomatoes", "c_tomatoes", "inflation_tomatoes", "trust_tomatoes",
    "o_tomatoes_paste", "h_tomatoes_paste", "l_tomatoes_paste", "c_tomatoes_paste", "inflation_tomatoes_paste", "trust_tomatoes_paste",
    "o_wheat", "h_wheat", "l_wheat", "c_wheat", "inflation_wheat", "trust_wheat",
    "o_wheat_flour", "h_wheat_flour", "l_wheat_flour", "c_wheat_flour", "inflation_wheat_flour", "trust_wheat_flour",
    "o_yogurt", "h_yogurt", "l_yogurt", "c_yogurt", "inflation_yogurt", "trust_yogurt",
    "o_food_price_index", "h_food_price_index", "l_food_price_index", "c_food_price_index", "inflation_food_price_index", "trust_food_price_index"
]

def lambda_handler(event, context):
    output = []
    succeeded = 0
    failed = 0

    for record in event["records"]:
        record_id = record["recordId"]

        try:
            # 1) decode base64 -> bytes
            payload_bytes = base64.b64decode(record["data"])

            # 2) bytes -> string
            payload_str = payload_bytes.decode("utf-8")

            # 3) string -> dict
            obj = json.loads(payload_str)

            # 4) dict -> ordered row (missing keys become "")
            row = [obj.get(col, "") for col in FIELD_ORDER]

            # 5) row -> CSV line (proper quoting)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(row)
            csv_line = buf.getvalue()  # includes newline

            # 6) csv -> base64 string
            data_b64 = base64.b64encode(csv_line.encode("utf-8")).decode("utf-8")

            output.append({"recordId": record_id, "result": "Ok", "data": data_b64})
            succeeded += 1

        except Exception:
            
            output.append({"recordId": record_id, "result": "ProcessingFailed", "data": record["data"]})
            failed += 1

    print(f"Processing completed. Successful {succeeded}, Failed {failed}.")
    return {"records": output}