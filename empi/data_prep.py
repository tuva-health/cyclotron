

# --------------------------------------------------
# Import necessary packages
import pandas as pd
import numpy as np
import re
import unicodedata



# --------------------------------------------------
# Read in the data
df = pd.read_csv("tuva_synth_clean.csv", dtype=str)
df.head()
df.shape()



# --------------------------------------------------
# Custom EDA

# Look for records with null 'source_person_id'
df[df['source_person_id'].isnull()]

# Remove records with null 'source_person_id'
df = df[df['source_person_id'].notnull()]
df.shape

# Look, eg, at how many data sources you have:
df['data_source'].unique()






# --------------------------------------------------
# Remove any columns you don't want to use:
df = df.drop(columns=["data_source","county"])

# Or keep only specific columns:

# List of columns you want to keep
columns_to_keep = ['source_person_id','first_name', 'last_name', 'sex','race',
                   'birth_date','death_date','social_security_number','address',
                   'city','state','zip_code','phone']

# Create the new DataFrame with just those columns
df = df_full[columns_to_keep]





# --------------------------------------------------
# Check for duplicates:
df.duplicated().sum()

# Remove duplicate records and check how many records we have left after:
df = df.drop_duplicates().reset_index(drop=True)
df.shape






# --------------------------------------------------
# Group by 'source_person_id','data_source' to see if sometimes
# the same person from the same data source can appear in
# multiple records with variations in their demographic fields
grouped = (
    df.groupby(['source_person_id','data_source'])
          .size()
          .reset_index(name='counter')
          .sort_values(by='counter',ascending=False)
)

grouped.head(5)


# Look at an example of a source_person_id that appears on
# multiple records, potentially with variations in demographic fields
df[df['source_person_id'] == 123456789]









# Now we do data transformation to prepare fields for matching,
# one at a time




# **************************************************
# source_person_id
# **************************************************

# See how many nulls we have
df['source_person_id'].isnull().sum()

# See how many source_person_ids we have of different lengths
df['source_person_id'].dropna().astype(str).str.len().value_counts().sort_index()








# **************************************************
# data_source
# **************************************************

# See the distinct values the data_source field takes
df['data_source'].unique()

# See if the data_source field has null values
df['data_source'].isnull().sum()








# **************************************************
# first_name
# **************************************************

# See how many nulls we have
df['first_name'].isnull().sum()


def clean_first_name(df: pd.DataFrame) -> pd.DataFrame:
    nickname_map = {
        "bob": "robert",
        "bobby": "robert",
        "rob": "robert",
        "robby": "robert",
        "rick": "richard",
        "ricky": "richard",
        "rich": "richard",
        "dick": "richard",
        "bill": "william",
        "billy": "william",
        "will": "william",
        "willy": "william",
        "liz": "elizabeth",
        "beth": "elizabeth",
        "betsy": "elizabeth",
        "lisa": "elizabeth",
        "kate": "katherine",
        "katie": "katherine",
        "kathy": "katherine",
        "jen": "jennifer",
        "jenny": "jennifer",
        "mike": "michael",
        "mikey": "michael",
        "chris": "christopher",
        "topher": "christopher",
        "dan": "daniel",
        "danny": "daniel",
        "steve": "steven",
        "stevie": "steven",
        "jim": "james",
        "jimmy": "james",
        "jamie": "james",
        "johnny": "john",
        "johnnie": "john",
        "jack": "john",
        "andy": "andrew",
        "drew": "andrew",
        "pat": "patrick",
        "trish": "patricia",
        "tricia": "patricia",
        "sam": "samuel",
        "samantha": "sam",
        "toni": "antonia",
        "tony": "anthony",
    }

    def normalize_name(name):
        if pd.isnull(name):
            return None
        # Lowercase and strip
        name = name.lower().strip()
        # Normalize accented characters
        name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
        # Remove non-alpha characters
        name = re.sub(r"[^a-z\s\-]", "", name)
        # Collapse whitespace
        name = re.sub(r"\s+", " ", name)
        # Map nickname
        if name in nickname_map:
            name = nickname_map[name]
        # Return None if empty
        return name if name else None

    df["first_name"] = df["first_name"].apply(normalize_name)
    return df


df = clean_first_name(df)








# **************************************************
# last_name
# **************************************************

# See how many nulls we have
df['last_name'].isnull().sum()


def clean_last_name(df: pd.DataFrame) -> pd.DataFrame:
    # Common surname prefixes to normalize spacing
    family_prefixes = {
        "de", "del", "de la", "van", "von", "da", "di", "la", "le", "du", "st", "mac", "mc"
    }

    def normalize_name(name):
        if pd.isnull(name):
            return None

        # Step 1: Normalize unicode (é → e), lowercase, strip whitespace
        name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
        name = name.lower().strip()

        # Step 2: Remove non-alphabetic characters except space and hyphen
        name = re.sub(r"[^a-z\s\-]", "", name)
        name = re.sub(r"\s+", " ", name)

        # Step 3: Normalize spacing after prefix (if present)
        for prefix in sorted(family_prefixes, key=lambda x: -len(x)):  # longer first
            if name.startswith(prefix + " "):
                # Keep spacing; just ensure it's normalized and clean
                name = prefix + " " + name[len(prefix):].strip()
                break

        return name if name else None

    df["last_name"] = df["last_name"].apply(normalize_name)
    return df

df = clean_last_name(df)








# **************************************************
# sex
# **************************************************

# See how many nulls we have
df['sex'].isnull().sum()


# See distinct values
df['sex'].unique()


# See counts of distinct values
df['sex'].value_counts(dropna=False)


# Convert all occurrences of 'unknown' into null:
df['sex'] = df['sex'].replace('unknown', np.nan)








# **************************************************
# race
# **************************************************

# See how many nulls we have
df['race'].isnull().sum()


# See distinct values
df['race'].unique()


# See counts of distinct values
df['race'].value_counts(dropna=False)


# Convert all occurrences of 'unknown' into null:
df['race'] = df['race'].replace('unknown', np.nan)
df['race'] = df['race'].replace('asked but unknown', np.nan)








# **************************************************
# birth_date
# **************************************************

# See how many nulls we have
df['birth_date'].isnull().sum()


# See distinct values
df['birth_date'].unique()


# See counts of distinct values
df['birth_date'].value_counts(dropna=False)


# Check whether all non-null values have the correct format
df['birth_date'].dropna().astype(str).str.fullmatch(r'\d{4}-\d{2}-\d{2}').all()


# See how many values we have of different lengths
df['birth_date'].dropna().astype(str).str.len().value_counts().sort_index()








# **************************************************
# death_date
# **************************************************

# See how many nulls we have
df['death_date'].isnull().sum()


# See distinct values
df['death_date'].unique()


# See counts of distinct values
df['death_date'].value_counts(dropna=False)


# Check whether all non-null values have the correct format
df['death_date'].dropna().astype(str).str.fullmatch(r'\d{4}-\d{2}-\d{2}').all()


# See how many values we have of different lengths
df['death_date'].dropna().astype(str).str.len().value_counts().sort_index()








# **************************************************
# social_security_number
# **************************************************

# See how many nulls we have
df['social_security_number'].isnull().sum()


# See how many values we have of different lengths
df['social_security_number'].dropna().astype(str).str.len().value_counts().sort_index()



def clean_social_security_number(df: pd.DataFrame) -> pd.DataFrame:
    def normalize_ssn(ssn):
        if pd.isnull(ssn):
            return None
        # Convert to string and strip whitespace
        ssn = str(ssn).strip()
        # Remove trailing ".0" if present
        if ssn.endswith(".0"):
            ssn = ssn[:-2]
        # Remove non-digit characters
        ssn = re.sub(r"[^\d]", "", ssn)
        # Keep only if exactly 9 digits
        if len(ssn) == 9:
            return ssn
        return None

    df["social_security_number"] = df["social_security_number"].apply(normalize_ssn)
    return df

df = clean_social_security_number(df)








# **************************************************
# address
# **************************************************

# See how many nulls we have
df['address'].isnull().sum()


def normalize_address(addr):
    if pd.isnull(addr):
        return addr  # preserve nulls

    addr = addr.lower().strip()

    # Remove punctuation
    addr = re.sub(r'[^\w\s]', '', addr)  # remove , . # - etc.

    # Normalize PO Box patterns
    addr = re.sub(r'\b(post office|p[\s\.]?o[\s\.]?)\s*box\b', 'po box', addr)

    # Normalize street suffixes
    addr = re.sub(r'\bstreet\b', 'st', addr)
    addr = re.sub(r'\bavenue\b', 'ave', addr)
    addr = re.sub(r'\broad\b', 'rd', addr)
    addr = re.sub(r'\bdrive\b', 'dr', addr)
    addr = re.sub(r'\bboulevard\b', 'blvd', addr)
    addr = re.sub(r'\blane\b', 'ln', addr)
    addr = re.sub(r'\btrail\b', 'trl', addr)
    addr = re.sub(r'\bplace\b', 'pl', addr)
    addr = re.sub(r'\bsquare\b', 'sq', addr)
    addr = re.sub(r'\bcourt\b', 'ct', addr)

    # Normalize unit and building descriptors
    addr = re.sub(r'\bapartment\b', 'apt', addr)
    addr = re.sub(r'\bapt\b', 'apt', addr)
    addr = re.sub(r'\bsuite\b', 'ste', addr)
    addr = re.sub(r'\bunit\b', 'unit', addr)
    addr = re.sub(r'\bbuilding\b', 'bldg', addr)
    addr = re.sub(r'\broom\b', 'rm', addr)

    # Normalize directions
    addr = re.sub(r'\bnorth\b', 'n', addr)
    addr = re.sub(r'\bsouth\b', 's', addr)
    addr = re.sub(r'\beast\b', 'e', addr)
    addr = re.sub(r'\bwest\b', 'w', addr)

    # Remove common stop words (add more if needed)
    addr = re.sub(r'\b(the|at|and|of|for)\b', '', addr)

    # Collapse multiple spaces into one
    addr = re.sub(r'\s+', ' ', addr)

    return addr.strip()


df['address'] = df['address'].apply(normalize_address)








# **************************************************
# city
# **************************************************

# See how many nulls we have
df['city'].isnull().sum()


def normalize_city_column(df, column='city'):
    """
    Clean and standardize the 'city' column of a DataFrame for record linkage.

    Steps:
    - Lowercase all values
    - Trim leading/trailing and extra internal spaces
    - Remove punctuation and special characters
    - Standardize known city variants
    - Replace junk values like 'unknown' with NaN
    """
    city_aliases = {
        'nyc': 'new york',
        'n.y.c.': 'new york',
        'sf': 'san francisco',
        'la': 'los angeles',
    }

    # Convert to string, lowercase, and trim
    df[column] = df[column].astype(str).str.lower().str.strip()

    # Remove punctuation
    df[column] = df[column].str.replace(r'[^\w\s]', '', regex=True)

    # Normalize spacing
    df[column] = df[column].str.replace(r'\s+', ' ', regex=True)

    # Replace known aliases
    df[column] = df[column].replace(city_aliases)

    # Replace junk values with NaN
    df[column] = df[column].replace(
        ['unknown', 'n/a', 'null', '', 'nan'], np.nan
    )

    return df


df = normalize_city_column(df)








# **************************************************
# state
# **************************************************

# See how many nulls we have
df['state'].isnull().sum()

# See unique values
df['state'].unique()


def clean_state_column(df, column='state'):
    """
    Cleans the state column by replacing any value not in the list
    of valid U.S. state and territory abbreviations with NaN.
    """
    valid_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI',
        'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI',
        'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC',
        'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
        'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'AS', 'GU', 'MP', 'PR', 'VI',
        'AB','BC','MB','NB','NL',
        'NS','ON','PE','QC','SK',
        'NT','NU','YT'
    }

    # Clean the column: uppercase and strip whitespace
    df[column] = df[column].astype(str).str.strip().str.upper()

    # Replace values not in valid_states with NaN
    df[column] = df[column].where(df[column].isin(valid_states), np.nan)

    return df


df = clean_state_column(df)


# See counts of distinct values
df['state'].value_counts(dropna=False)








# **************************************************
# zip_code
# **************************************************

# See how many nulls we have
df['zip_code'].isnull().sum()

# See unique values
df['zip_code'].unique()


# See how many we have of different lengths
df['zip_code'].dropna().astype(str).str.len().value_counts().sort_index()


def clean_zip_code_column(df, column='zip_code', extract_5_digit=True):
    """
    Cleans a ZIP code column by:
    - Converting to string
    - Removing trailing '.0'
    - Removing all non-digit characters
    - Replacing junk values
    - Keeping only 5- or 9-digit ZIPs
    - Optionally truncating to 5 digits
    """
    # Convert to string and strip whitespace
    df[column] = df[column].astype(str).str.strip()

    # Remove trailing '.0'
    df[column] = df[column].str.replace(r'\.0$', '', regex=True)

    # Remove non-digit characters
    df[column] = df[column].str.replace(r'\D', '', regex=True)

    # Replace known junk values
    df[column] = df[column].replace({'00000': np.nan, '99999': np.nan, '12345': np.nan})

    # Keep only ZIPs of length 5 or 9
    df[column] = df[column].where(df[column].str.len().isin([5, 9]), np.nan)

    # Optionally truncate to first 5 digits
    if extract_5_digit:
        df[column] = df[column].str[:5]

    return df


df = clean_zip_code_column(df)








# **************************************************
# phone
# **************************************************

# See how many nulls we have
df['phone'].isnull().sum()

# See unique values
df['phone'].unique()


# See how many we have of different lengths
df['phone'].dropna().astype(str).str.len().value_counts().sort_index()

# See counts of distinct values
df['phone'].value_counts(dropna=False)



def clean_phone_column(df, column='phone', extract_10_digits=True):
    """
    Cleans a phone number column by:
    - Converting to string
    - Removing trailing '.0'
    - Removing non-digit characters
    - Replacing invalid or placeholder numbers
    - Keeping only 10-digit phone numbers (optional)
    """
    # Convert to string and strip
    df[column] = df[column].astype(str).str.strip()

    # Remove trailing '.0'
    df[column] = df[column].str.replace(r'\.0$', '', regex=True)

    # Remove all non-digit characters
    df[column] = df[column].str.replace(r'\D', '', regex=True)

    # Replace known junk or placeholder numbers
    df[column] = df[column].replace({
        '0000000000': np.nan,
        '1234567890': np.nan,
        '1111111111': np.nan
    })

    # Optionally ensure it's 10 digits (standard US phone length)
    if extract_10_digits:
        df[column] = df[column].where(df[column].str.len() == 10, np.nan)

    return df


df = clean_phone_column(df)











# **************************************************
# Write the clean data to a CSV
# **************************************************

df.to_csv('clean_data.csv', index=False)

