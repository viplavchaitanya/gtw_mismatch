import io
import re
import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple, Optional

# =========================================
# 1. Page & Session State Initialization
# =========================================
st.set_page_config(page_title="NTAMC Mapping & Interoperability Validator", layout="wide")

_defaults = {
    "upload_key_xml": 0,
    "upload_key_xls": 0,
    "results_ready": False,
    "base_df_head": None,
    "validated_df": None,
    "val_summary": None,
    "df_addr": None,
    "df_gtwprot": None,
    "enriched_bytes": None,
    "enriched_fname": "NTAMC Signal List – enriched.xlsx",
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

st.title("NTAMC Mapping & Interoperability Validator")

# =========================================
# 2. Shared Helper Functions
# =========================================
def clean_path(text: str) -> str:
    if not text: return ''
    parts = [p.strip() for p in re.split(r"\r?\n+", text) if p.strip()]
    return ' / '.join(parts)

def as_int(val: Any) -> Optional[int]:
    if val is None or (isinstance(val, float) and pd.isna(val)): return None
    s = str(val).strip()
    if re.fullmatch(r'[+-]?\d+', s):
        try: return int(s)
        except: return None
    if re.fullmatch(r'[+-]?\d+\.\d+', s):
        try:
            f = float(s)
            return int(f) if f.is_integer() else None
        except: return None
    return None

def find_attr(attrs: List[Any], ids=None, names=None, contains_name=None, return_desc=False) -> str:
    ids, names, contains_name = ids or [], names or [], contains_name or []
    for a in attrs:
        if a.attrib.get('id', '') in ids or a.attrib.get('Name', '') in names:
            return a.attrib.get('Desc', '') if return_desc else a.attrib.get('Value', '')
    for a in attrs:
        aname = a.attrib.get('Name', '')
        if any(k.lower() in aname.lower() for k in contains_name):
            return a.attrib.get('Desc', '') if return_desc else a.attrib.get('Value', '')
    return ''

def get_ioa(attrs: List[Any]) -> Optional[int]:
    ids = ['AddressObjetField1', 'AddressObjectField1', 'ObjectAddress', 'ObjectAddr', 'AddrCommonT104']
    return as_int(find_attr(attrs, ids=ids, contains_name=['address']))

# =========================================
# 3. Requirement Functions
# =========================================

def requirement_base_extraction(xml_bytes: bytes, path_filter: str, apply_filter: bool) -> pd.DataFrame:
    rows = []
    context = ET.iterparse(io.BytesIO(xml_bytes), events=('end',))
    for _, elem in context:
        if elem.tag != 'Object' or 'address' not in elem.attrib.get('idType', '').lower():
            continue
        
        ext_el = elem.find('ExtId')
        path = clean_path(ext_el.text if ext_el is not None else '')
        if apply_filter and path_filter and path_filter not in path:
            elem.clear(); continue

        attrs = [a for a in list(elem) if a.tag == 'Attribute']
        rels = [r for r in list(elem) if r.tag == 'Relation']
        
        scada_addrs = sorted(set(clean_path(r.find('TargetObjectExtId').text) for r in rels if r.find('TargetObjectExtId') is not None))
        scada_str = ' \n'.join(filter(None, scada_addrs))
        
        if not scada_str.strip():
            elem.clear(); continue

        rows.append({
            'path': path,
            'short name': as_int(find_attr(attrs, ids=['ShortName'])),
            'type': elem.attrib.get('idType', ''),
            'field 1 address by default': get_ioa(attrs),
            'SCADA address': scada_str
        })
        elem.clear()
    
    df = pd.DataFrame(rows)
    return df.sort_values(['path', 'short name']).reset_index(drop=True) if not df.empty else df

def requirement_mapping_validation(df: pd.DataFrame, ref_ports: int) -> Tuple[pd.DataFrame, Dict]:
    res = df.copy()
    res['remarks'] = ""
    
    mask = res['short name'] != res['field 1 address by default']
    res.loc[mask, 'remarks'] = "Short Name and Adress Mismatch"
    
    counts = res['field 1 address by default'].value_counts()
    mask_port = res['field 1 address by default'].map(counts) != ref_ports
    res.loc[mask_port, 'remarks'] = res.apply(lambda x: (x['remarks'] + "; " if x['remarks'] else "") + f"Gateway mapping is to be done to all {ref_ports} ports", axis=1)
    
    summary = {"total_rows": len(res), "total_rows_with_issues": (res['remarks'] != "").sum(), "reference_ports": ref_ports}
    return res, summary

def requirement_gateway_interop_validation(xml_bytes: bytes, path_filter: str, apply_filter: bool) -> Tuple[pd.DataFrame, pd.DataFrame]:
    addr_rows, gtw_rows = [], []
    context = ET.iterparse(io.BytesIO(xml_bytes), events=('end',))
    
    for _, elem in context:
        obj_type = elem.attrib.get('idType', '')
        ext_el = elem.find('ExtId')
        path = clean_path(ext_el.text if ext_el is not None else '')
        if apply_filter and path_filter and path_filter not in path:
            elem.clear(); continue
            
        attrs = [a for a in list(elem) if a.tag == 'Attribute']
        sn = find_attr(attrs, ids=['ShortName'])
        ioa = get_ioa(attrs)

        if "Address" in obj_type:
            actual_soe = find_attr(attrs, ids=['FlgSOE'], return_desc=True)
            if obj_type == 'GtwSCADAMVAddress' and actual_soe != "Yes without time tag":
                addr_rows.append({"path": path, "short name": sn, "ioa": ioa, "rule": "MV: FlgSOE", "expected": "Yes without time tag", "actual": actual_soe, "status": "FAIL"})
            elif ('SPS' in obj_type or 'DPS' in obj_type) and actual_soe != "Yes with time tag":
                addr_rows.append({"path": path, "short name": sn, "ioa": ioa, "rule": "SPS/DPS: FlgSOE", "expected": "Yes with time tag", "actual": actual_soe, "status": "FAIL"})

        elif obj_type == 'GtwProt':
            for attr_id, exp in [("T104_W", 8), ("T104_K", 12), ("T104_T1", 15)]:
                act = as_int(find_attr(attrs, ids=[attr_id]))
                if act != exp:
                    gtw_rows.append({"path": path, "rule": attr_id, "expected": exp, "actual": act, "status": "FAIL"})
        elem.clear()
    return pd.DataFrame(addr_rows), pd.DataFrame(gtw_rows)

# =========================================
# 4. Workbook Enrichment Logic (Integrated)
# =========================================
def requirement_enrich_signal_list(xls_bytes: bytes, base_df: pd.DataFrame, val_df: pd.DataFrame, addr_df: pd.DataFrame, slice_lit: str) -> bytes:
    """Combines all discrepancies and writes them into the NTAMC Excel template."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Load the original sheets
        with pd.ExcelFile(io.BytesIO(xls_bytes)) as xls:
            for sheet in xls.sheet_names:
                df_original = pd.read_excel(xls, sheet_name=sheet)
                
                # Logic: If it's the main signal sheet, add the 'Descrepency' column
                if "Signal" in sheet or "List" in sheet:
                    # Create a lookup map for discrepancies
                    mismatch_map = val_df[val_df['remarks'] != ""].set_index('field 1 address by default')['remarks'].to_dict()
                    df_original['Descrepency'] = df_original['Address'].map(mismatch_map).fillna("")
                
                df_original.to_excel(writer, sheet_name=sheet, index=False)
                
        # Add a new summary sheet for Interop issues
        if not addr_df.empty:
            addr_df.to_excel(writer, sheet_name="Interop_Issues", index=False)
            
    return output.getvalue()

# =========================================
# 5. Main Execution
# =========================================
def main():
    with st.sidebar:
        st.header("Settings")
        apply_filter = st.checkbox("Apply PATH filter", value=True)
        keyword = st.text_input("Filter Keyword", value="AMC")
        ref_ports = st.number_input("Reference Ports", value=6)
        slice_lit = st.text_input("NTAMC Path Slice", value="GTW1 | NTAMC |")

    xml_file = st.file_uploader("Upload SCADA XML", type=["xml"], key=f"xml_{st.session_state.upload_key_xml}")
    xls_file = st.file_uploader("Upload Signal List", type=["xlsx"], key=f"xls_{st.session_state.upload_key_xls}")
    
    if st.button("Run Validation and Mapping", use_container_width=True):
        if not xml_file:
            st.error("XML file is required.")
            return

        xml_data = xml_file.getvalue()
        
        # 1. Base Extraction
        base_df = requirement_base_extraction(xml_data, keyword, apply_filter)
        if base_df.empty:
            st.warning("No data found with the selected filters.")
            return
            
        # 2. Mapping Validation
        val_df, val_sum = requirement_mapping_validation(base_df, int(ref_ports))
        
        # 3. Interop Validation
        df_addr, df_gtw = requirement_gateway_interop_validation(xml_data, keyword, apply_filter)
        
        # 4. Enrichment (if XLS exists)
        enriched = None
        if xls_file:
            enriched = requirement_enrich_signal_list(xls_file.read(), base_df, val_df, df_addr, slice_lit)

        st.session_state.update({
            "results_ready": True,
            "base_df_head": base_df.head(10),
            "validated_df": val_df,
            "val_summary": val_sum,
            "enriched_bytes": enriched
        })
        st.rerun()

    if st.session_state.results_ready:
        st.subheader("Summary")
        st.write(st.session_state.val_summary)
        
        if st.session_state.enriched_bytes:
            st.download_button("Download Enriched Signal List", st.session_state.enriched_bytes, file_name="Enriched_NTAMC_List.xlsx")
        
        if st.button("Clear Results"):
            for k in _defaults: st.session_state[k] = _defaults[k]
            st.rerun()

if __name__ == "__main__":
    main()