import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine, text
import spacy
from unidecode import unidecode
import spacy
import re
import os

def get_il_placements(transactions):
    transactions['description'] = transactions['description'].str.lower()
    transactions = transactions.sort_values(by='effectivedate')
    transactions['is_il_placement'] = (
    (transactions['description'].str.contains("injured list|disabled list")) &
    ~(transactions['description'].str.contains("activated|reinstated|returned|transferred|recalled")))

    transactions['next_trans_is_il_placement'] = transactions.groupby(['person_id'])['is_il_placement'].shift(-1)
    
    injury = transactions[(transactions['is_il_placement']) & ~(transactions['next_trans_is_il_placement'] == True)]
    injury = injury.drop_duplicates(subset=['person_id','effectivedate'])
    new_columns = {'trans_id':'il_place_trans_id', 'person_id':'person_id', 'effectivedate':'il_place_date','description':'il_place_description','toteam_id':'il_place_team'}
    injury = injury.rename(columns=new_columns)
    injury = injury.loc[:,list(new_columns.values())]
    return injury

def setup_injury_parser():
    """Build a ready-to-use spaCy NLP pipeline for parsing injury descriptions.
    Returns:
        nlp: spaCy NLP pipeline with custom EntityRuler
        parse_injury_text: function(text) -> dict with structured fields
    """
    # Dictionary to normalize different injury terms into canonical labels
    injury_normalizer = {
        "rupture":"tear",
        "ruptured":"tear",
        "tear": "tear",
        "torn": "tear",
        "tommy john":"tear",

        "partial tear":"partial tear",
        "partially torn": "partial tear",
        
        "damage":"partial tear",

        "thoracic outlet": "thoracic outlet syndrome",
        "tos":'thoracic outlet syndrome',

        'blood clot':'blood clot',
        'effusion':'effusion',
        'herniation':"hernia",
        "hernia":"hernia",
        "herniated":"hernia",
        "spur":"bone spur",
        "bone chips":"bone spur",
        "bone spur":"bone spur",
        "bone spurs":"bone spur",
        "loose bodies":"bone spur",
        "loose body":"bone spur",
        "subluxed":"subluxation",
        "subluxation":"subluxation",
        "pinched nerve":"pinched nerve",
        "neuritis":"neuritis",
        "stiffness": "stiffness",
        "spasm":"spasm",
        "spasms":"spasm",
        "fatigue":"fatigue",
        "blisters":"blister",
        "blister":"blister",
        "repair":"surgery",
        "construction":"surgery",
        "reconstruction":"surgery",
        "replacement":"surgery",
        "surgery":"surgery",
        "bruised":"contusion",
        "bruise":"contusion",
        "contusion":"contusion",
        "impinged":"impingement",
        "impingement":"impingement",
        "strain": "strain",
        "strained": "strain",
        "strains":"strain",
        "sprain": "sprain",
        "sprains":"sprain",
        "sprained": "sprain",
        "dislocation":"dislocation",
        "dislocated":"dislocation",
        "break":"fracture",
        "broken": "fracture",
        "fracture": "fracture",
        "fractured": "fracture",
        "fractures":"fracture",
        "epicondylitis":"inflammation",
        "inflamation":"inflammation",
        "irritation":"inflammation",
        "inflammation": "inflammation",
        "inflammtion":"inflammation",
        "inflamed": "inflammation",
        "tendinopathy":"tendinitis",
        "tendinits": "tendinitis",
        "tendinitis": "tendinitis",
        "tendonitis": "tendinitis",
        "stress reaction":"stress reaction",
        "bursitis": "bursitis",
        "tightness": "tightness",
        'tenderness':'soreness',
        "sore":"soreness",
        "soreness": "soreness",
        "stiff":"stiffness",
        "stiffness":"stiffness",
        "swelling":"swelling",
        "weakness":"weakness",
        "pain": "pain",
        "discomfort": "discomfort",
        "concussion": "concussion",
        "hyperextension":"hyperextension",
        "hyperextended":"hyperextension",
        "plantar fasciitis":"plantar fasciitis",
        "plantar fascitis":"plantar fasciitis",
        "laceration":"laceration",
        "lacerated":"laceration",
        "abrasion":"laceration",
        "turf":"turf toe",

        "recovery":"rehab",
        "recovering":"rehab",
        "rehab":"rehab",

        "injury":"undisclosed",
        "injured":"undisclosed",

        "vertigo":"vertigo",
        "laceration":"laceration",

        "non-baseball": "non-baseball medical condition",
        "virus": "non-baseball medical condition",
        "viral":"non-baseball medical condition",
        "migraines":"non-baseball medical condition",
        "cancer":"non-baseball medical condition",
        "irregular heartbeat":"non-baseball medical condition",
        "illness":"non-baseball medical condition",
        "infection":"non-baseball medical condition",
        "influenza":"non-baseball medical condition",
        "appendicitis":"non-baseball medical condition",
        "hand foot mouth disease":"non-baseball medical condition",
        "gastrointeritis":"non-baseball medical condition",
        "anxiety":"non-baseball medical condition"

    }

    # Dictionary to normalize different body part names
    body_part_normalizer = {
        "eye":"eye",
        "eyes":"eye",
        "skull":"head",
        "facial":"head",
        'head':'head',
        "cervical":"neck",
        "neck": "neck",
        "shoulder": "shoulder",
        "rotator cuff": "rotator cuff",
        "ac joint": "ac joint",
        "labrum": "labrum",
        "teres major":"teres major",
        "scapular":"scapular",
        'sc joint':'sc joint', 
        "ulnar":"elbow",
        "elbow": "elbow",
        "pronator":"forearm",
        "forearm": "forearm",
        "flexor":"forearm",
        "extensor":"forearm",
        "ucl":"ucl",
        "ulnar colateral ligament":"ucl",
        "ulnar collateral ligament":"ucl",
        "tommy john":"ucl",
        "biceps":"bicep",
        "bicep":"bicep",
        "tricep":"tricep",
        "triceps":"tricep",
        "arm":"arm",
        "trapezius":"trap",
        "trap": "trap",
        "lat":"lat",
        "latissimus":"lat",

        "wrist": "wrist",
        "hamate":"hand",
        "hand": "hand",
        "finger": "finger",
        "thumb": "thumb",

        "nerve":"nerve",
        "pec": "chest",
        "pectoral": "chest",
        "abdominal":"abdomen",
        "abdomen":"abdomen",
        "core":"abdomen",
        "side":"oblique",
        "oblique": "oblique",
        "intercostal": "rib",
        "costochondral":"rib",
        "ribcage":"rib",
        "rib": "rib",
        "ribs":"rib",
        "spine":"back",
        "spinal":"back",
        "lumbar":"back",
        "back": "back",

        "hamstring": "hamstring",
        "groin": "groin",
        "quad": "quad",
        "quadriceps": "quad",
        "quadricep": "quad",
        "si joint": "si joint",
        "hip": "hip",
        "adductor":"adductor",
        "abductor":"abductor",
        "glute": "glute",
        "lower leg":"calf",
        "calf": "calf",
        "achilles": "achilles",
        "meniscus":"meniscus",
        "anterior cruciate ligament":"achilles",
        "acl":"achilles",
        "patella":"patellar",
        "patellar": "patellar",
        "knees":"knee",
        "knee":"knee",
        "mcl":"knee",
        "shin":"shin",
        "tibia":"shin",
        "fibula":"shin",
        "ankle": "ankle",
        "heel":"foot",
        "foot": "foot",
        "toe": "toe",
        
    }

    # Build lists of recognized terms
    injury_terms = list(injury_normalizer.keys())
    body_parts = list(body_part_normalizer.keys())
    sides = ["left", "right"]

    # ✅ Load spaCy and add EntityRuler
    nlp = spacy.load("en_core_web_sm")
    ruler = nlp.add_pipe("entity_ruler", before="ner")

    # Create patterns for each term
    patterns = []
    # Sides
    for s in sides:
        patterns.append({"label": "SIDE", "pattern": s})
    # Body parts
    for bp in body_parts:
        patterns.append({"label": "BODY_PART", "pattern": bp})
    # Injury types
    for it in injury_terms:
        patterns.append({"label": "INJURY_TYPE", "pattern": it})

    ruler.add_patterns(patterns)

    # ---------- Priority system (minimal, self-contained) ----------
    PRIORITY = [  # NEW: ordered high -> low
        "partial tear", "tear", "surgery","fracture", "eye","dislocation", "subluxation", "thoracic outlet syndrome", 'effusion',
        "sprain", "strain", "tendinitis", "neuritis", "bone spur", "inflammation", "contusion",
        "impingement", "spasm", "tightness", "soreness", "pain", "bursitis", "hernia", "pinched nerve",
        "discomfort", "fatigue", "concussion", "blister", "plantar fasciitis", "stiffness", "stress reaction", "hyperextension",
        'blood clot', "laceration",  "vertigo", "weakness", "swelling", "turf toe",
        "non-baseball medical condition","undisclosed"
    ]

    RANK = {cls: i for i, cls in enumerate(PRIORITY)}  # NEW: label -> rank


    BODY_PRIORITY = [
        "thoracic outlet", "labrum", "rotator cuff", "ac joint", "si joint" ,"teres major", 'sc joint',
        "thumb", "ucl", "lat", "shoulder", "forearm", "achilles",
        "meniscus","wrist", "finger","hand", "bicep", "tricep", "lat", "chest", "rib", "neck", "back",
        "oblique",'adductor', 'abductor', "hip", "groin", "quad", "hamstring", "glute",
       "patellar", "knee", "calf",  "ankle", "foot", "toe", "plantar fascia",
        "trap", "scapular", "shin", "elbow", "nerve", "arm", "abdomen", 'head'
        
    ]
    BODY_RANK = {bp: i for i, bp in enumerate(BODY_PRIORITY)}

    def choose_body_part(cands):              # NEW
        known = [c for c in cands if c in BODY_RANK]
        if not known:
            return None
        return min(known, key=lambda c: BODY_RANK[c])



    def choose_injury(cands):  # NEW: deterministic selector
        known = [c for c in cands if c in RANK]
        if not known:
            return None
        return min(known, key=lambda c: RANK[c])
    # ----------------------------------------------------------------

    # Define function to apply parser to text
    def parse_injury_text(text: str):
        doc = nlp(text.lower())
        side, body_part = None, None
        injuries_found = []
        body_parts_found = []  # NEW: collect all candidates

        for ent in doc.ents:
            if ent.label_ == "SIDE":
                side = ent.text
            elif ent.label_ == "BODY_PART":
                body_parts_found.append(      # CHANGED: accumulate
                    body_part_normalizer.get(ent.text, ent.text)
                )
            elif ent.label_ == "INJURY_TYPE":
                injuries_found.append(  # CHANGED: accumulate instead of overwrite
                    injury_normalizer.get(ent.text, ent.text)
                )

        if re.search(r"\btommy john\b", doc.text):
            if body_part is None:
                body_parts_found.append("ucl")     # <-- FIX 2: add to the candidate list
                injuries_found.append("tear")
        
        if "elbow" in body_parts_found and "surgery" in injuries_found:
            body_parts_found.append("ucl")     # <-- FIX 2: add to the candidate list
            injuries_found.append("tear")

        if "concussion" in injuries_found and "head" not in body_parts_found:
            body_parts_found.append("head")

        
        if "thoracic outlet syndrome" in injuries_found:
            body_parts_found.append("shoulder")

        if "plantar fasciitis" in injuries_found:
            body_parts_found.append("foot")

        if "oblique" in body_parts_found and injuries_found == []:
            injuries_found.append("strain")

        body_part   = choose_body_part(body_parts_found)
        injury_type = choose_injury(injuries_found)  # NEW: pick by priority

        return {
            "side": side,
            "body_part": body_part,
            "injury_type": injury_type
        }

    return nlp, parse_injury_text

def create_raw_injury(injury, mlb_teams, mlb_players):
    injury = injury.merge(mlb_players, how='left', on='person_id')
    injury = injury.loc[~injury.fullname.isnull() & ~injury.secondary_name.isnull()]
    injury.loc[:, 'fullname'] = injury['fullname'].apply(unidecode).str.lower()
    injury['secondary_name'] = injury['secondary_name'].apply(unidecode).str.lower()
    injury['il_place_description'] = injury['il_place_description'].apply(unidecode)
    injury.loc[:,'raw_injury'] = injury.apply(lambda row : row['il_place_description'].replace(str(row['fullname']), '').replace(str(row['secondary_name']), ''), axis=1)
    teams_remove = r'\b(?:{})\b'.format('|'.join( mlb_teams['name'].str.lower().unique()))
    positions = ['p', 'shp', 'rhp','lhp', 'c', '1b', '2b', '3b','ss', 'lf', 'cf', 'rf', 'dh']
    positions_remove = r'\b(?:{})\b'.format('|'.join(positions))
    words = ['placed', 'injured', 'disabled', '7-day', '10-day', '15-day', '60-day', 'list', 'on', 'the', 'retroactive', 'recovering', 'from', 'right', 'left', 'to', 'recovery', 'his', 'of', 'knew', 'with', 'associated', 'with',
         'symptoms', 'th', 'in', 'day', 'retro', 'and', 'jr', 'related','original', 'a', 'reoccurring']
    words_pat = r'\b(?:{})\b'.format('|'.join(words))
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
    month_pat = r'\b(?:{})\b'.format('|'.join(months))
    misspelled = ['jiman choi', 'mike soroka','sammy long', 'dee gordon','hyunjin ryu', 'troutd', 'zach britton','juan perez','j pollock','josh h smith', 'dwight smith']
    misspelled_pat = r'\b(?:{})\b'.format('|'.join(misspelled))
    injury.loc[:,'raw_injury'] = injury['raw_injury'].replace(teams_remove, '', regex=True).replace(positions_remove, '', regex=True).replace(words_pat, '', regex=True).replace(r'\d+', '', regex=True).replace(month_pat, '', regex=True)\
    .replace(misspelled_pat, '', regex=True)
    injury.loc[:,'raw_injury'] = injury['raw_injury'].str.replace(r'[^a-zA-Z\s]', '', regex=True).replace(r'\s+', ' ', regex=True).str.strip()
    injury.loc[:,'raw_injury'] = injury.apply(lambda row : row['raw_injury'].replace(str(row['fullname']), '').replace(str(row['secondary_name']), ''), axis=1)
    injury.loc[:,'raw_injury'] = injury.apply(lambda row : row['raw_injury'].replace(str(row['fullname']), '').replace(str(row['secondary_name']), ''), axis=1).replace(misspelled_pat, '', regex=True)
    injury.loc[injury.raw_injury == '', 'raw_injury'] = 'undisclosed'
    return injury


def assume_side(row):
    arm_injuries = ['shoulder','roator cuff','ac joint', 'labrum', 'teres major', 'scapular', 'sc joint', 'ulnar', 'elbow',
                    'forearm','ucl','bicep','tricep','arm','lat','pec','wrist','hand','finger']
    if (pd.notna(row.side)) or (row.primaryposition_code != '1' and row.primaryposition_code != 'Y') or ((row.body_part not in arm_injuries) and (row.body_part != 'oblique')):
        return row.side
    if row.body_part in arm_injuries:
        if row.pitchhand_code == 'R':
            return 'right'
        else:
            return 'left'
    elif row.body_part == 'oblique':
        if row.pitchhand_code == 'R':
            return 'left'
        else:
            return 'right'

def body_part_groups(injury):
    body_part_groups = {'shoulder': ['shoulder','rotator cuff', 'ac joint', 'labrum','teres major','scapular','sc joint'],
        'elbow':['elbow','forearm', 'ucl','nerve'],
        'other arm': ['arm','bicep','tricep','lat','chest','trap'],
        'hand':['wrist','hand','finger','thumb'],
        'head': ['head','eye','neck'],
        'torso':['abdomen','oblique','rib','back'],
        'knee':['knee','achilles','meniscus','patellar'],
        'upper leg':["hamstring", "groin", "quad", "si joint","hip","adductor","abductor","glute"],
        'lower leg':["calf","shin","ankle","foot","toe"]}
    for key in body_part_groups.keys():
        injury.loc[injury.body_part.isin(body_part_groups[key]), 'body_part_group'] = key
    return injury

def create_il_placements(transactions, mlb_teams, mlb_players, engine):
    injury = get_il_placements(transactions)
    injury = create_raw_injury(injury, mlb_teams, mlb_players)
    nlp, parse_injury_text = setup_injury_parser()
    parsed = injury["il_place_description"].apply(parse_injury_text)
    parsed_df = pd.DataFrame(parsed.tolist())
    injury = pd.concat([injury.reset_index(drop=True), parsed_df.reset_index(drop=True)], axis=1)
    injury['side'] = injury.apply(assume_side, axis=1)
    injury = body_part_groups(injury)
    injury = injury.drop(columns=['fullname','secondary_name','primaryposition_code','pitchhand_code'])
    injury.loc[injury.raw_injury == 'undisclosed', ['side', 'body_part', 'injury_type']] = 'undisclosed'
    injury.to_sql('il_placements', engine, schema='silver', index=False, if_exists='replace')
    return




if __name__ == "__main__":
    BASEBALL_URL = os.environ['BASEBALL_URL']
    engine = create_engine(BASEBALL_URL)
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Connected to PostgreSQL!")
            print(f"PostgreSQL version: {result.fetchone()[0]}")
    except Exception as e:
        print("Failed to connect to PostgreSQL:")
        print(e)
        exit()
    transactions_query = "SELECT * FROM silver.transactions where description is not null and person_id is not null"
    transactions = pd.read_sql(transactions_query, engine)
    mlb_teams_query = 'select id, name from bronze.teams;'
    mlb_teams = pd.read_sql(mlb_teams_query, engine)
    mlb_players_query = 'select distinct person_id, fullname, secondary_name, primaryposition_code, pitchhand_code from silver.players;'
    mlb_players = pd.read_sql(mlb_players_query, engine)
    create_il_placements(transactions, mlb_teams, mlb_players, engine)