# app.py
import io
import re
import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple, Optional

# =========================
# Page & Session State init
# =========================
st.set_page_config(page_title="NTAMC Mapping & Interoperability Validator", layout="wide")

# -- Session state keys for persistence across reruns (so downloads don't "reset" the page)
_defaults = {
    "upload_key_xml": 0,
    "upload_key_xls": 0,
    "results_ready": False,
    "base_df_head": None,
    "validated_df": None,
    "val_summary": None,
    "df_addr": None,
    "df_gtwprot": None,
    "gtw_summary": None,
    "scada_xlsx_bytes": None,
    "gtw_xlsx_bytes": None,
    "enriched_bytes": None,
    "enriched_fname": "NTAMC Signal List – enriched.xlsx",  # will be overridden if XLS uploaded
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

st.title("NTAMC Mapping & Interoperability Validator")

# =========================
# UI: Sidebar configuration
# =========================
with st.sidebar:
    st.header("Options")
    apply_path_filter = st.checkbox("Apply PATH filter on XML", value=True)
    filter_keyword = st.text_input("PATH filter keyword", value="AMC")

    # Default reference = 6 (override manual)
    reference_ports = st.number_input(
        "Reference ports (override)", min_value=1, max_value=100000, value=6, step=1,
        help="Used by mapping validation: each IOA must appear exactly this many times."
    )

    ntamc_path_slice_literal = st.text_input("Restrict 'Mapping in database' to PATH containing", value="GTW1 | NTAMC |")
    st.caption("• Base Extractor (First Filter) is to eliminate RLDC signals.\n"
               "• 'Mapping in database' is to select NTAMC port for merging with NTAMC List.\n"
               "• Reference ports are the number of T104 ports in gateway (excluding RLDC). Default Value is 6")

# =========================
# File Uploaders (with resettable keys)
# =========================
xml_file = st.file_uploader(
    "Upload SCADA XML", type=["xml"], key=f"xml_{st.session_state.upload_key_xml}"
)
xls_file = st.file_uploader(
    "Upload NTAMC Signal List (.xlsx)", type=["xlsx"], key=f"xls_{st.session_state.upload_key_xls}"
)

# New label requested
run_btn = st.button("Run Validation steps and Map to Signal List", use_container_width=True)

# =================================
# Shared helpers (from your modules)
# =================================
def clean_path(text: str) -> str:
    if not text:
        return ''
    parts = [p.strip() for p in re.split(r"\r?\n+", text) if p.strip()]
    return ' / '.join(parts)

def find_attr(attrs: List[Any], *, ids=None, names=None, contains_name=None, return_desc=False) -> str:
    ids = ids or []
    names = names or []
    contains_name = contains_name or []
    for a in attrs:  # exact match
        aid = a.attrib.get('id', '')
        aname = a.attrib.get('Name', '')
        if aid in ids or aname in names:
            return a.attrib.get('Desc', '') if return_desc else a.attrib.get('Value', '')
    for a in attrs:  # contains
        aname = a.attrib.get('Name', '')
        if any(k.lower() in aname.lower() for k in contains_name):
            return a.attrib.get('Desc', '') if return_desc else a.attrib.get('Value', '')
    return ''

def as_int(val: Any) -> Optional[int]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if re.fullmatch(r'[+-]?\d+', s):
        try:
            return int(s)
        except Exception:
            return None
    if re.fullmatch(r'[+-]?\d+\.\d+', s):
        try:
            f = float(s)
            return int(f) if f.is_integer() else None
        except Exception:
            return None
    return None

def try_to_int(x: Any) -> Any:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return x
    s = str(x).strip()
    if re.fullmatch(r'[+-]?\d+', s):
        try: return int(s)
        except Exception: return x
    if re.fullmatch(r'[+-]?\d+\.\d+', s):
        try:
            f = float(s)
            if f.is_integer():
                return int(f)
        except Exception:
            pass
    return x

def spare_to_yesno(v: Any) -> Any:
    if v is None:
        return v
    s = str(v).strip()
    if s == '0': return 'No'
    if s == '1': return 'Yes'
    try:
        f = float(s)
        if f == 0: return 'No'
        if f == 1: return 'Yes'
    except Exception:
        pass
    return v

ADDRESS_IDS_NUMERIC = [
    'AddressObjetField1', 'AddressObjectField1', 'ObjectAddress', 'ObjectAddr',
    'AddrObjetField1', 'AddressObjet', 'AddressField1', 'AddrCommonT104'
]
ADDRESS_IDS_NET = ['AddressTCPIP', 'AddressTCPIPSubnet', 'AddressTCPIPGateway']
ADDRESS_NAMES_NET = ['TCP/IP address', 'Subnetwork Mask', 'Default Gateway']

def extract_field1_address(attrs: List[Any]) -> str:
    val = find_attr(attrs, ids=ADDRESS_IDS_NUMERIC)
    if val: return val
    return find_attr(attrs, ids=ADDRESS_IDS_NET, names=ADDRESS_NAMES_NET, contains_name=['address'])

def invert_value_by_type(obj_type: str, attrs: List[Any]) -> str:
    t = obj_type.strip()
    if t == 'GtwSCADASPSAddress':
        inv_raw = find_attr(attrs, ids=['Inversion','Invert','Inv'], contains_name=['invert','inversion'])
        return 'Yes' if inv_raw == '1' else ('No' if inv_raw == '0' else '')
    elif t in ('GtwSCADASPCAddress', 'GtwSCADADPCAddress'):
        return find_attr(attrs, ids=['TypeSCADA'], return_desc=True)
    elif t == 'GtwSCADAMVAddress':
        return find_attr(attrs, ids=['format','Format'], return_desc=True)
    elif t == 'GtwSCADADPSAddress':
        return find_attr(attrs, ids=['FlgInv'], return_desc=True)
    else:
        return find_attr(attrs, ids=['Inversion','Invert','Inv'], contains_name=['invert','inversion'])

# =========================================
# 1) Base extractor (GTW_Mismatch equivalent)
# =========================================
def build_base_df(xml_bytes: bytes, path_filter: Optional[str]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    context = ET.iterparse(io.BytesIO(xml_bytes), events=('end',))
    for event, elem in context:
        if elem.tag != 'Object':
            continue
        obj_type = elem.attrib.get('idType','')
        if 'address' not in obj_type.lower():
            elem.clear(); continue

        ext_el = elem.find('ExtId')
        path = clean_path(ext_el.text if ext_el is not None else '')

        attrs = [a for a in list(elem) if a.tag == 'Attribute']
        rels  = [r for r in list(elem) if r.tag == 'Relation']

        target_ext_ids = []
        for r in rels:
            tgt = r.find('TargetObjectExtId')
            if tgt is not None and (tgt.text or '').strip():
                target_ext_ids.append(clean_path(tgt.text))

        seen = set()
        scada_address: List[str] = []
        for v in target_ext_ids:
            if v not in seen:
                seen.add(v)
                scada_address.append(v)
        scada_address_str = ' \n'.join(scada_address)
        

        # >>> NEW: Ignore entries with empty SCADA address <<<
        
        if scada_address_str.strip() == "":
            elem.clear()
            continue


        short_name = find_attr(attrs, ids=['ShortName'], names=['short name'])
        spare_val  = find_attr(attrs, ids=['Spare'], names=['spare'])
        addr_val   = extract_field1_address(attrs)
        inversion  = invert_value_by_type(obj_type, attrs)

        rows.append({
            'path': path,
            'short name': short_name,
            'type': obj_type,
            'spare': spare_val,
            'field 1 address by default': addr_val,
            'Inversion': inversion,
            'SCADA address': scada_address_str
        })
        elem.clear()

    df = pd.DataFrame(rows)

    if apply_path_filter and path_filter:
        df = df[df['path'].str.contains(path_filter, na=False)]

    if 'type' in df.columns:
        df = df[df['type'].str.contains('Address', case=False, na=False)]

    if 'spare' in df.columns:
        df['spare'] = df['spare'].apply(spare_to_yesno)

    if 'short name' in df.columns:
        df['short name'] = df['short name'].apply(try_to_int)
    if 'field 1 address by default' in df.columns:
        df['field 1 address by default'] = df['field 1 address by default'].apply(try_to_int)

    sort_cols = [c for c in ['path', 'short name', 'type'] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    return df

# =======================================
# 2) Mapping validator (with reference override)
# =======================================
MSG_MISMATCH_SHORT_ADDR = "Short Name and Adress Mismatch"
MSG_INCOMPLETE_PORTS    = "Gateway mapping is to be done to all {ref} ports"
MSG_INV_NOT_EVEN        = "Control type/Inversion is not even across all ports"
MSG_MAP_NOT_UNIFORM     = "Mapping is not uniform across all ports"

def _normalize_value_for_compare(v: Any):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if s.isdigit() or (s.startswith(('+','-')) and s[1:].isdigit()):
        try: return int(s)
        except Exception: pass
    try:
        f = float(s); 
        if f.is_integer(): return int(f)
    except Exception:
        pass
    return s

def _uniform_across_ports(values: pd.Series) -> bool:
    s = values.astype(str).str.strip()
    empties = s.eq('') | values.isna()
    if empties.all():  # all blank -> OK
        return True
    if empties.any():  # mixed blank/non-blank -> mismatch
        return False
    return s.nunique() == 1

def determine_reference_from_address(df: pd.DataFrame) -> Tuple[int, Dict[int,int], List[str]]:
    notes: List[str] = []
    addr_counts_series = df['field 1 address by default'].value_counts(dropna=False)
    hist = addr_counts_series.value_counts().to_dict()
    candidate_counts = sorted(set(addr_counts_series.values) & {3, 6})
    if candidate_counts:
        reference = max(candidate_counts)
    else:
        reference = int(addr_counts_series.max())
        notes.append(f"No address groups with size 3 or 6 found; falling back to global max count = {reference}")
    return reference, hist, notes

def validate_mappings(df_in: pd.DataFrame, ref_override: Optional[int] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df_in.copy()
    if 'remarks' not in df.columns:
        df['remarks'] = ""

    s1 = df['short name'].apply(_normalize_value_for_compare)
    s2 = df['field 1 address by default'].apply(_normalize_value_for_compare)
    mask_short_addr = (s1 != s2)
    df.loc[mask_short_addr, 'remarks'] = df.loc[mask_short_addr, 'remarks'].mask(
        df['remarks'].eq(""), MSG_MISMATCH_SHORT_ADDR
    ).where(df['remarks'].eq(""), df['remarks'] + "; " + MSG_MISMATCH_SHORT_ADDR)

    derived_ref, addr_hist, notes = determine_reference_from_address(df)
    reference = int(ref_override) if (ref_override and ref_override > 0) else int(derived_ref)

    addr_key = df['field 1 address by default'].astype(object)
    addr_counts = addr_key.value_counts(dropna=False)
    count_map = addr_key.map(addr_counts)
    mask_bad_rep = (count_map != reference)
    df.loc[mask_bad_rep, 'remarks'] = df.loc[mask_bad_rep, 'remarks'].mask(
        df['remarks'].eq(""), MSG_INCOMPLETE_PORTS.format(ref=reference)
    ).where(df['remarks'].eq(""), df['remarks'] + "; " + MSG_INCOMPLETE_PORTS.format(ref=reference))

    inv_mismatch_indices = []
    scada_mismatch_indices = []
    for addr_val, grp in df.groupby('field 1 address by default', dropna=False):
        if not _uniform_across_ports(grp['Inversion']):
            inv_mismatch_indices.extend(grp.index.tolist())
        if not _uniform_across_ports(grp['SCADA address']):
            scada_mismatch_indices.extend(grp.index.tolist())

    if inv_mismatch_indices:
        idx = pd.Index(inv_mismatch_indices)
        df.loc[idx, 'remarks'] = df.loc[idx, 'remarks'].mask(
            df['remarks'].eq(""), MSG_INV_NOT_EVEN
        ).where(df['remarks'].eq(""), df['remarks'] + "; " + MSG_INV_NOT_EVEN)

    if scada_mismatch_indices:
        idx = pd.Index(scada_mismatch_indices)
        df.loc[idx, 'remarks'] = df.loc[idx, 'remarks'].mask(
            df['remarks'].eq(""), MSG_MAP_NOT_UNIFORM
        ).where(df['remarks'].eq(""), df['remarks'] + "; " + MSG_MAP_NOT_UNIFORM)

    df['remarks'] = df['remarks'].fillna("").astype(str)

    summary: Dict[str, Any] = {
        'total_rows': int(len(df)),
        'total_rows_with_issues': int((df['remarks'] != "").sum()),
        'reference_ports': int(reference),
        'address_group_size_histogram': addr_hist,
        'notes': notes + ([f"Reference overridden to {reference}"] if ref_override else [])
    }
    return df, summary

# ======================================================
# 3) Gateway validators (address + GtwProt) with IOA key
# ======================================================
def extract_ioa(attrs: List[Any]) -> Optional[int]:
    v = find_attr(attrs, ids=ADDRESS_IDS_NUMERIC, contains_name=['address'])
    return as_int(v)

def validate_gateway_and_port_params(xml_bytes: bytes, path_filter: Optional[str]) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    address_rows: List[Dict[str, Any]] = []
    gtwprot_rows: List[Dict[str, Any]] = []

    def pass_fail_row(path: str, short_name: Any, obj_type: str, ioa: Optional[int],
                      rule: str, expected: str, actual: str, ok: bool) -> Dict[str, Any]:
        return {
            "path": path,
            "short name": short_name,
            "type": obj_type,
            "ioa": ioa,
            "rule": rule,
            "expected": expected,
            "actual": actual,
            "status": "PASS" if ok else "FAIL",
            "remarks": "" if ok else f"Expected [{expected}] but found [{actual}]"
        }

    ctx = ET.iterparse(io.BytesIO(xml_bytes), events=('end',))
    for event, elem in ctx:
        if elem.tag != 'Object':
            continue
        obj_type = elem.attrib.get('idType','').strip()
        ext_el = elem.find('ExtId')
        path = clean_path(ext_el.text if ext_el is not None else '')
        if apply_path_filter and path_filter and (path_filter not in path):
            elem.clear(); continue

        attrs = [a for a in list(elem) if a.tag == 'Attribute']
        short_name = find_attr(attrs, ids=['ShortName'], names=['short name'])
        ioa = extract_ioa(attrs)

        # Address rules
        if obj_type in (
            'GtwSCADAMVAddress','GtwSCADASPSAddress','GtwSCADADPSAddress',
            'GtwSCADASPCAddress','GtwSCADADPCAddress'
        ):
            def desc(attr_id=None, name=None, contains=None) -> str:
                return find_attr(attrs,
                                 ids=[attr_id] if attr_id else [],
                                 names=[name] if name else [],
                                 contains_name=[contains] if contains else [],
                                 return_desc=True) or ""

            if obj_type == 'GtwSCADAMVAddress':
                d = desc('FlgSOE', name='FlgSOE')
                address_rows.append(pass_fail_row(path, short_name, obj_type, ioa,
                    "MV: FlgSOE.Desc", "Yes without time tag", d, d.strip().lower()=="yes without time tag"))
                d = desc('EventRecord', name='EventRecord')
                address_rows.append(pass_fail_row(path, short_name, obj_type, ioa,
                    "MV: EventRecord.Desc", "No", d, d.strip().lower()=="no"))
                d = desc('CycleType', name='CycleType')
                address_rows.append(pass_fail_row(path, short_name, obj_type, ioa,
                    "MV: CycleType.Desc", "Periodic", d, d.strip().lower()=="periodic"))
                d = (desc('Format', name='Format') or desc('format', name='format'))
                address_rows.append(pass_fail_row(path, short_name, obj_type, ioa,
                    "MV: Format.Desc contains 'float'", "contains 'float'", d, "float" in d.strip().lower()))

            elif obj_type in ('GtwSCADASPSAddress','GtwSCADADPSAddress'):
                d = desc('FlgSOE', name='FlgSOE')
                address_rows.append(pass_fail_row(path, short_name, obj_type, ioa,
                    "SPS/DPS: FlgSOE.Desc", "Yes with time tag", d, d.strip().lower()=="yes with time tag"))

            elif obj_type == 'GtwSCADASPCAddress':
                d = desc('TypeSCADA', name='TypeSCADA', contains='TypeSCADA')
                address_rows.append(pass_fail_row(path, short_name, obj_type, ioa,
                    "SPC: TypeSCADA.Desc", "Direct execute", d, d.strip().lower()=="direct execute"))

            elif obj_type == 'GtwSCADADPCAddress':
                d = desc('TypeSCADA', name='TypeSCADA', contains='TypeSCADA')
                address_rows.append(pass_fail_row(path, short_name, obj_type, ioa,
                    "DPC: TypeSCADA.Desc", "Select execute", d, d.strip().lower()=="select execute"))

        # GtwProt rules
        elif obj_type == 'GtwProt':
            def val(attr_id=None, name=None) -> Optional[int]:
                v = find_attr(attrs, ids=[attr_id] if attr_id else [], names=[name] if name else [])
                return as_int(v)
            def val_any(ids_or_names: List[str]) -> Optional[int]:
                for key in ids_or_names:
                    v = find_attr(attrs, ids=[key], names=[key])
                    iv = as_int(v)
                    if iv is not None: return iv
                return None
            def desc(attr_id=None, name=None) -> str:
                return find_attr(attrs, ids=[attr_id] if attr_id else [], names=[name] if name else [], return_desc=True) or ""

            checks_value = [
                ("GtwProt: AddrCommon or AddrCommonT104.Value", "AddrCommon|AddrCommonT104", 1,
                 val_any(["AddrCommon","AddrCommonT104"])),
                ("GtwProt: CycleBS or CycleBST104.Value", "CycleBS|CycleBST104", 60,
                 val_any(["CycleBS","CycleBST104"])),
                ("GtwProt: CycleTm or CycleTmT104.Value", "CycleTm|CycleTmT104", 15,
                 val_any(["CycleTm","CycleTmT104"])),
                ("GtwProt: StrucAddrObjet or StrucAddrObjet_T104.Value", "StrucAddrObjet|StrucAddrObjet_T104", 7,
                 val_any(["StrucAddrObjet","StrucAddrObjet_T104"])),
                ("GtwProt: T104_W.Value", "T104_W", 8, val("T104_W")),
                ("GtwProt: T104_T3.Value", "T104_T3", 20, val("T104_T3")),
                ("GtwProt: T104_T2.Value", "T104_T2", 10, val("T104_T2")),
                ("GtwProt: T104_T1.Value", "T104_T1", 15, val("T104_T1")),
                ("GtwProt: T104_T0.Value", "T104_T0", 30, val("T104_T0")),
                ("GtwProt: T104_K.Value", "T104_K", 12, val("T104_K")),
            ]
            for rule_label, aid, exp, got in checks_value:
                gtwprot_rows.append({
                    "path": path, "short name": short_name, "type": obj_type, "ioa": None,
                    "rule": rule_label, "expected": str(exp), "actual": str(got) if got is not None else "MISSING",
                    "status": "PASS" if got == exp else "FAIL",
                    "remarks": "" if got == exp else f"Expected [{exp}] but found [{got}]"
                })
            d_tm = desc("TimeManagement")
            gtwprot_rows.append({
                "path": path, "short name": short_name, "type": obj_type, "ioa": None,
                "rule": "GtwProt: TimeManagement.Desc", "expected": "Local", "actual": d_tm,
                "status": "PASS" if d_tm.strip().lower() == "local" else "FAIL",
                "remarks": "" if d_tm.strip().lower() == "local" else f"Expected [Local] but found [{d_tm}]"
            })
            d_pt = desc("ProtocolType")
            gtwprot_rows.append({
                "path": path, "short name": short_name, "type": obj_type, "ioa": None,
                "rule": "GtwProt: ProtocolType.Desc", "expected": "T104", "actual": d_pt,
                "status": "PASS" if d_pt.strip().lower() == "t104" else "FAIL",
                "remarks": "" if d_pt.strip().lower() == "t104" else f"Expected [T104] but found [{d_pt}]"
            })
        elem.clear()

    df_addr = pd.DataFrame(address_rows, columns=[
        "path", "short name", "type", "ioa", "rule", "expected", "actual", "status", "remarks"
    ]).sort_values(["type","path","short name","rule"]).reset_index(drop=True)
    df_gtwprot = pd.DataFrame(gtwprot_rows, columns=[
        "path", "short name", "type", "ioa", "rule", "expected", "actual", "status", "remarks"
    ]).sort_values(["path","short name","rule"]).reset_index(drop=True)

    summary = {
        "address_checks_total": int(len(df_addr)),
        "address_checks_fail": int((df_addr['status'] == "FAIL").sum()) if not df_addr.empty else 0,
        "gtwprot_checks_total": int(len(df_gtwprot)),
        "gtwprot_checks_fail": int((df_gtwprot['status'] == "FAIL").sum()) if not df_gtwprot.empty else 0,
    }
    return df_addr, df_gtwprot, summary

# =========================
# 4) Enrichment utilities
# =========================
def _to_int_or_none(x: Any):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if s == "": return None
    try:
        f = float(s)
        if f.is_integer(): return int(f)
        return None
    except Exception:
        pass
    try:
        return int(s)
    except Exception:
        return None

def build_gateway_mismatch_map(df_validated: pd.DataFrame) -> Dict[int, str]:
    tmp = df_validated.copy()
    tmp['ioa'] = tmp['field 1 address by default'].apply(_to_int_or_none)
    tmp = tmp.dropna(subset=['ioa'])
    tmp = tmp[tmp['remarks'].astype(str).str.strip() != ""]
    if tmp.empty: return {}
    agg = (tmp.groupby('ioa')['remarks']
             .apply(lambda s: '; '.join(sorted(set(v.strip() for v in s if v.strip()))))
             .reset_index())
    return {int(k): v for k, v in zip(agg['ioa'], agg['remarks'])}

def build_scada_map_for_ntamc(base_df: pd.DataFrame, slice_literal: str) -> Dict[int, str]:
    pattern = re.escape(slice_literal)
    df = base_df[base_df['path'].str.contains(pattern, na=False, regex=True)].copy()
    df['ioa'] = df['field 1 address by default'].apply(_to_int_or_none)
    df = df.dropna(subset=['ioa'])
    df['SCADA address'] = df['SCADA address'].astype(str).str.strip()
    df = df[df['SCADA address'] != ""]
    if df.empty: return {}
    agg = (df.groupby('ioa')['SCADA address']
             .apply(lambda s: '\n'.join(sorted(set(v for v in s if v)))))
    return {int(k): v for k, v in agg.items()}

def build_interoperability_map_failed_only(df_addr: pd.DataFrame) -> Dict[int, str]:
    x = df_addr.dropna(subset=['ioa']).copy()
    if x.empty: return {}
    x['ioa'] = x['ioa'].astype(int)
    def fail_text(g: pd.DataFrame) -> str:
        failed = g.loc[g['status'].astype(str).str.upper() == 'FAIL', 'remarks'].astype(str).str.strip().unique()
        return '; '.join(failed) if len(failed) > 0 else ""  # blank if all pass
    agg = x.groupby('ioa').apply(fail_text).reset_index(name='failed_rules')
    return {int(r['ioa']): r['failed_rules'] for _, r in agg.iterrows()}

def build_scada_validation_xlsx(validated_df: pd.DataFrame, summary: Dict[str, Any]) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        validated_df.to_excel(writer, index=False, sheet_name='SCADA (validated)')
        rows = [
            ['reference_ports', summary.get('reference_ports')],
            ['total_rows', summary.get('total_rows')],
            ['rows_with_issues', summary.get('total_rows_with_issues')],
            ['-- address_group_size_histogram --', 'count_of_addresses'],
        ]
        for k, v in sorted(summary.get('address_group_size_histogram', {}).items()):
            rows.append([f'group_size={k}', v])
        if summary.get('notes'):
            rows.append(['-- notes --', ''])
            for n in summary['notes']:
                rows.append([n, ''])
        pd.DataFrame(rows, columns=['metric','value']).to_excel(writer, index=False, sheet_name='Summary')
    out.seek(0)
    return out.getvalue()

def build_gateway_validations_xlsx(df_addr: pd.DataFrame, df_gtwprot: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        df_addr.to_excel(writer, index=False, sheet_name='Address_Validations')
        if not df_gtwprot.empty and 'status' in df_gtwprot.columns:
            df_gtwprot_fail = df_gtwprot[df_gtwprot['status'].astype(str).str.upper() == 'FAIL'].copy()
        else:
            df_gtwprot_fail = df_gtwprot.copy()
        df_gtwprot_fail.to_excel(writer, index=False, sheet_name='GtwProt_Validations')
    out.seek(0)
    return out.getvalue()

def enrich_ntamc_workbook(xls_bytes: bytes,
                          base_df: pd.DataFrame,
                          df_validated: pd.DataFrame,
                          df_addr: pd.DataFrame,
                          df_gtwprot: pd.DataFrame,
                          ntamc_slice_literal: str) -> bytes:
    mismatch_map = build_gateway_mismatch_map(df_validated)
    scada_map    = build_scada_map_for_ntamc(base_df, ntamc_slice_literal)
    interop_map  = build_interoperability_map_failed_only(df_addr)

    xls = pd.ExcelFile(io.BytesIO(xls_bytes), engine='openpyxl')
    descrepencies_rows: List[pd.DataFrame] = []

    out_buffer = io.BytesIO()
    with pd.ExcelWriter(out_buffer, engine='openpyxl') as writer:
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')
            if 'NTAMC 104 address' in df.columns:
                ioa_series = df['NTAMC 104 address'].apply(_to_int_or_none)
                df['Gateway mismatch Remarks'] = ioa_series.map(lambda k: mismatch_map.get(k, "") if k is not None else "")
                df['Mapping in database']      = ioa_series.map(lambda k: scada_map.get(k, "") if k is not None else "")
                df['Interoperability Remarks'] = ioa_series.map(lambda k: interop_map.get(k, "") if k is not None else "")

                mask_desc = (df['Gateway mismatch Remarks'].astype(str).str.strip() != "") | \
                            (df['Interoperability Remarks'].astype(str).str.strip() != "")
                if mask_desc.any():
                    df_d = df.loc[mask_desc].copy()
                    df_d.insert(0, 'Sheet', sheet)
                    descrepencies_rows.append(df_d)

            df.to_excel(writer, sheet_name=sheet, index=False)

        # Append GtwProt_Validations (FAIL only)
        if not df_gtwprot.empty and 'status' in df_gtwprot.columns:
            df_gtwprot_fail = df_gtwprot[df_gtwprot['status'].astype(str).str.upper() == 'FAIL'].copy()
        else:
            df_gtwprot_fail = df_gtwprot.copy()
        df_gtwprot_fail.to_excel(writer, sheet_name='GtwProt_Validations', index=False)

        # descrepencies sheet combining all sheets
        if descrepencies_rows:
            df_desc_all = pd.concat(descrepencies_rows, ignore_index=True)
        else:
            df_desc_all = pd.DataFrame(columns=['Sheet','NTAMC 104 address',
                                                'Gateway mismatch Remarks','Interoperability Remarks','Mapping in database'])
        df_desc_all.to_excel(writer, sheet_name='descrepencies', index=False)

    out_buffer.seek(0)
    return out_buffer.getvalue()

# =========================
# Reset helper
# =========================
def reset_all():
    # increment uploader keys to clear file widgets
    st.session_state.upload_key_xml += 1
    st.session_state.upload_key_xls += 1
    # clear computed artifacts
    for k in ["results_ready","base_df_head","validated_df","val_summary","df_addr","df_gtwprot",
              "gtw_summary","scada_xlsx_bytes","gtw_xlsx_bytes","enriched_bytes","enriched_fname"]:
        st.session_state[k] = _defaults.get(k)
    st.rerun()

# =========================
# Main run
# =========================
# If we already have results from a previous run, show the downloads & previews first (so downloads don't "reset" the page)
if st.session_state["results_ready"] and not run_btn:
    st.success("Results are ready. You can download outputs or Reset & start a new run.")
    if st.session_state["base_df_head"] is not None:
        st.dataframe(st.session_state["base_df_head"], use_container_width=True)

    if st.session_state["val_summary"] is not None and st.session_state["gtw_summary"] is not None:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Mapping validation summary")
            st.write({
                "total_rows": st.session_state["val_summary"]["total_rows"],
                "rows_with_issues": st.session_state["val_summary"]["total_rows_with_issues"],
                "reference_ports (in use)": st.session_state["val_summary"]["reference_ports"],
            })
        with col2:
            st.subheader("Gateway validations summary")
            st.write({
                "address_checks_total": st.session_state["gtw_summary"]["address_checks_total"],
                "address_checks_fail": st.session_state["gtw_summary"]["address_checks_fail"],
                "gtwprot_checks_total": st.session_state["gtw_summary"]["gtwprot_checks_total"],
                "gtwprot_checks_fail": st.session_state["gtw_summary"]["gtwprot_checks_fail"],
            })

    st.subheader("Downloads — Stepwise")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.session_state["scada_xlsx_bytes"]:
            st.download_button(
                label="Download SCADA validation.xlsx",
                data=st.session_state["scada_xlsx_bytes"],
                file_name="scada_validation.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    with c2:
        if st.session_state["gtw_xlsx_bytes"]:
            st.download_button(
                label="Download gateway_validations.xlsx",
                data=st.session_state["gtw_xlsx_bytes"],
                file_name="gateway_validations.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    with c3:
        if st.session_state["enriched_bytes"]:
            st.download_button(
                label=f"Download {st.session_state['enriched_fname']}",
                data=st.session_state["enriched_bytes"],
                file_name=st.session_state["enriched_fname"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    st.divider()
    st.button("Reset & start new", on_click=reset_all, use_container_width=True)

# Fresh run
if run_btn:
    if xml_file is None:
        st.error("Please upload the SCADA XML to proceed.")
        st.stop()

    xml_bytes = xml_file.read()
    xls_bytes = xls_file.read() if xls_file is not None else None

    with st.spinner("Parsing XML and building base dataframe..."):
        base_df = build_base_df(xml_bytes, filter_keyword if apply_path_filter else None)

    st.success(f"Base extraction complete. Rows: {len(base_df)}")
    st.session_state["base_df_head"] = base_df.head(10)

    with st.spinner("Running mapping validations..."):
        validated_df, val_summary = validate_mappings(base_df, ref_override=int(reference_ports))

    with st.spinner("Running gateway address & GtwProt validations..."):
        df_addr, df_gtwprot, gtw_summary = validate_gateway_and_port_params(xml_bytes, filter_keyword if apply_path_filter else None)

    # Build stepwise downloads & persist in session
    st.session_state["validated_df"] = validated_df
    st.session_state["val_summary"] = val_summary
    st.session_state["df_addr"] = df_addr
    st.session_state["df_gtwprot"] = df_gtwprot
    st.session_state["gtw_summary"] = gtw_summary

    st.session_state["scada_xlsx_bytes"] = build_scada_validation_xlsx(validated_df, val_summary)
    st.session_state["gtw_xlsx_bytes"]   = build_gateway_validations_xlsx(df_addr, df_gtwprot)

    # Enrich NTAMC only when NTAMC file is uploaded
    st.session_state["enriched_bytes"] = None
    st.session_state["enriched_fname"] = "NTAMC Signal List – enriched.xlsx"
    if xls_bytes is not None:
        with st.spinner("Enriching NTAMC workbook (with descrepencies)..."):
            enriched_bytes = enrich_ntamc_workbook(
                xls_bytes=xls_bytes,
                base_df=base_df,
                df_validated=validated_df,
                df_addr=df_addr,
                df_gtwprot=df_gtwprot,
                ntamc_slice_literal=ntamc_path_slice_literal
            )
        # Name as <UploadedName>_Mismatch.xlsx
        base_name = re.sub(r'\.xlsx$', '', xls_file.name, flags=re.IGNORECASE)
        st.session_state["enriched_fname"] = f"{base_name}_Mismatch.xlsx"
        st.session_state["enriched_bytes"] = enriched_bytes

    # Mark ready and rerun to stabilize UI (so downloading doesn't wipe state)
    st.session_state["results_ready"] = True
    st.rerun()