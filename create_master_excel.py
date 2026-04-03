"""
Generate CSRA_FeeID_Master.xlsx from the master tracking data.
Run once:  python create_master_excel.py
"""

import pandas as pd

# (Fee ID, Primary FS, FS Segment)
rows = [
    # ── DZ00006245 ──
    ("DZ00006245", "Physicians/Practitioners/Medical Clinics", "Anesthesia"),
    ("", "Physicians/Practitioners/Medical Clinics", "Certified Nurse Midwife"),
    ("", "Physicians/Practitioners/Medical Clinics", "Oral/Maxillofacial Surgeon"),
    ("", "Physicians/Practitioners/Medical Clinics", "Podiatry"),
    ("", "Physicians/Practitioners/Medical Clinics", "Physician Primary Care Rate Increase"),
    ("", "Physicians/Practitioners/Medical Clinics", "Practitioner"),
    ("", "Telemedicine", "Telemedicine Audio-Only"),
    ("", "Telemedicine", "Telemedicine Audio-Visual"),
    # ── CZ00034402 ──
    ("CZ00034402", "Chiropractor", "Chiropractor Fee Databases"),
    ("", "Clinical Laboratory", "Clinical Laboratory Fee Databases"),
    ("", "Medical Suppliers / Orthotists / Prosthetists / DME Dealers", "DMEPOS Database"),
    ("", "Family Planning", "Title X Family Planning Clinics"),
    ("", "Behavioral Health/Substance Abuse", "PIHP/CMHSP Physician Injectable Drugs Carve-Out"),
    ("", "Behavioral Health/Substance Abuse", ""),
    ("", "Behavioral Health/Substance Abuse", "Serious Emotional Disturbance (SED)"),
    ("", "Behavioral Health/Substance Abuse", "Non-Physician Behavioral Health"),
    ("", "Behavioral Health/Substance Abuse", "Children's Waiver Program"),
    ("", "Behavioral Health/Substance Abuse", "Targeted Case Management - Flint Waiver"),
    ("", "Behavioral Health/Substance Abuse", "Applied Behavior Analysis"),
    ("", "Clinic Institutional Billing", "Federally Qualified Health Center (FQHC)"),
    ("", "Clinic Institutional Billing", "Rural Health Clinic (RHC)"),
    ("", "Clinic Institutional Billing", "Tribal Health Center (THC)"),
    ("", "Urgent Care Centers", "Urgent Care Center Fee Databases"),
    ("", "Vision", "Vision Fee Database"),
    ("", "Maternal Infant Health Program", "Maternal Infant Health"),
    ("", "Local Health Department", "Local Health Department Fee Databases"),
    ("", "Hearing Services and Devices", "Hearing Aid Dealers Database"),
    ("", "Hearing Services and Devices", "Hearing Services Fee Databases"),
    ("", "Therapies", "Physical Therapy Fee Databases"),
    ("", "Therapies", "Occupational Therapy Fee Databases"),
    ("", "Therapies", "Speech Therapy Fee Databases"),
    # ── CZ00046142 ──
    ("CZ00046142", "Physicians/Practitioners/Medical Clinics", "Anesthesia"),
    ("", "Physicians/Practitioners/Medical Clinics", "Certified Nurse Midwife"),
    ("", "Physicians/Practitioners/Medical Clinics", "Oral/Maxillofacial Surgeon"),
    ("", "Physicians/Practitioners/Medical Clinics", "Podiatry"),
    ("", "Physicians/Practitioners/Medical Clinics", "Physician Primary Care Rate Increase"),
    ("", "Physicians/Practitioners/Medical Clinics", "Practitioner"),
    ("", "Telemedicine", "Telemedicine Audio-Only"),
    ("", "Telemedicine", "Telemedicine Audio-Visual"),
]

df = pd.DataFrame(rows, columns=["Fee ID", "Primary FS", "FS Segments"])

output = r"C:\Users\hemas\Downloads\Fee Schedule\CSRA_FeeID_Master.xlsx"

with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    df.to_excel(writer, sheet_name="MI", index=False)
    workbook = writer.book
    worksheet = writer.sheets["MI"]

    header_fmt = workbook.add_format({
        "bold": True, "font_color": "white", "bg_color": "#4472C4",
        "border": 1, "align": "center", "valign": "vcenter",
    })
    cell_fmt = workbook.add_format({"border": 1, "text_wrap": True, "valign": "vcenter"})

    for col_num, col_name in enumerate(df.columns):
        worksheet.write(0, col_num, col_name, header_fmt)
    for r_idx in range(len(df)):
        for c_idx in range(len(df.columns)):
            worksheet.write(r_idx + 1, c_idx, df.iloc[r_idx, c_idx], cell_fmt)

    worksheet.set_column("A:A", 16)
    worksheet.set_column("B:B", 52)
    worksheet.set_column("C:C", 50)

print(f"Created: {output}")
