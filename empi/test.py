import pandas as pd

# Make sure these are strings (or otherwise consistently typed)
# df = df.assign(
#     first_name=df["first_name"].astype(str),
#     last_name=df["last_name"].astype(str),
#     birth_date=df["birth_date"].astype(str),  # or keep as datetime if already parsed
#     social_security_number=df["social_security_number"].astype(str),
#     phone=df["phone"].astype(str),
#     address=df["address"].astype(str),
#     unique_id=df["unique_id"].astype(str)     # or int if you prefer
# )

def candidate_pairs_on(df, keys):
    """Return pairs (unique_id_aa, unique_id_bb) where rows share the given keys."""
    a = df[keys + ["unique_id"]].dropna(subset=keys).copy()
    b = a.copy()
    m = a.merge(b, on=keys, suffixes=("_aa", "_bb"))
    # Keep only distinct ordered pairs (aa.unique_id < bb.unique_id)
    m = m[m["unique_id_aa"] < m["unique_id_bb"]]
    return m[["unique_id_aa", "unique_id_bb"]].drop_duplicates()

# The seven OR rules (exactly your equality groups)
rule_keys = [
    ["first_name", "last_name", "birth_date"],
    ["social_security_number", "birth_date"],
    ["social_security_number", "last_name"],
    ["phone", "last_name", "birth_date"],
    ["address", "last_name", "birth_date"],
    ["first_name", "last_name", "phone"],
    ["first_name", "last_name", "address"],
]

pairs_list = [candidate_pairs_on(df, keys) for keys in rule_keys]
result_pairs = pd.concat(pairs_list, ignore_index=True).drop_duplicates()

# result_pairs is equivalent to:
# select aa.unique_id, bb.unique_id
# from df aa
# left join df bb
#   on aa.unique_id < bb.unique_id
#  and ( ... your 7 OR rules ... )
