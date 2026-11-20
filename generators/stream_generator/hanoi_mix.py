"""Assumptions and cohort denominators are documented in docs/hospital-profile.md."""

# Limits in local hours, and weights for outpatient registrations / ED arrivals.
# The ED is open around the clock. Weekend clinics receive people from 05:30.
ED_SHARE = 85000 / 1940000
TIME_BANDS = (
    (0, 5, 0, 0, 9), (5, 5.5, 3, 0, 1), (5.5, 7, 16, 18, 5),
    (7, 10, 54, 55, 15), (10, 12, 18, 18, 11), (12, 13, 0, 0, 5),
    (13, 16.5, 6, 9, 19), (16.5, 17, 1, 0, 3), (17, 20, 2, 0, 17),
    (20, 22, 0, 0, 9), (22, 24, 0, 0, 6),
)


def arrival_bands(weekend):
    for start, end, weekday_op, weekend_op, ed in TIME_BANDS:
        op = weekend_op if weekend else weekday_op
        share = ((1 - ED_SHARE) * op + ED_SHARE * ed) / 100
        yield start, end, share, ED_SHARE * ed / 100 / share


# Outpatient shares approximate diagnosis-code frequencies in the Hoàng Mai
# adult sample. The remainder pools chapters outside the modelled specialties.
# Older-age multipliers represent simulated associations, not fitted estimates.
OUTPATIENT = (
    ('Tiêu hóa', 21.1, .85), ('Nội tiết', 13.6, 1.12), ('Tim mạch', 9.7, 1.55),
    ('Cơ xương khớp', 9.1, 1.25), ('Thận – tiết niệu', 7.6, 1),
    ('Hô hấp', 6.6, 1), ('Thần kinh', 3.2, 1.15), ('Da liễu', 1.1, .7),
    ('Khám sức khỏe', 8.5, .7), ('Khác', 19.5, 1),
)
ED = (('Tiêu hóa', 7766), ('Truyền nhiễm', 2857), ('Chấn thương', 2651),
      ('Thần kinh', 2354), ('Tim mạch', 1478), ('Hô hấp', 1098),
      ('Thận – tiết niệu', 794), ('Cơ xương khớp', 394), ('Phản vệ', 294),
      ('Ung bướu', 148), ('Nội tiết', 80), ('Khác', 2471))


def outpatient_weights(senior):
    # Adjust by age while preserving the marginal group weights.
    denom = sum(w * factor for _, w, factor in OUTPATIENT)
    older = [100 * w * factor / denom for _, w, factor in OUTPATIENT]
    return older if senior else [(w - .283 * old) / .717 for (_, w, _), old in zip(OUTPATIENT, older)]


def patient_mix(rng, urgent):
    if urgent:
        age_band = rng.choices(((0, 17), (18, 49), (50, 95)), (1896, 12625, 7864))[0]
        group = rng.choices([g for g, _ in ED], [weight for _, weight in ED])[0]
    else:
        senior = rng.random() < .283
        age_band = (60, 95) if senior else (18, 59)
        weights = outpatient_weights(senior)
        group = rng.choices([g for g, _, _ in OUTPATIENT], weights)[0]
    return dict(age_years=rng.randint(*age_band), condition_group=group,
                care_setting='emergency' if urgent else 'outpatient')

def admission_probability(base, urgent, group):
    if urgent:
        return .2837
    average = (base - ED_SHARE * .2837) / (1 - ED_SHARE)
    # Relative likelihoods are scenario assumptions; normalize to the annual target.
    relative = {'Tim mạch': 1.65, 'Khám sức khỏe': .10}
    normalizer = sum(weight * relative.get(name, 1) for name, weight, _ in OUTPATIENT) / 100
    return average * relative.get(group, 1) / normalizer


def inpatient_ward(rng, urgent, group, specialty):
    severity = rng.random()
    if severity < (.09 if urgent else .033):
        return 'ICU'
    if severity < (.24 if urgent else .105):
        return 'EMERGENCY'
    if group == 'Tim mạch':
        return 'CARDIOLOGY'
    if group == 'Chấn thương' or specialty == 'Ngoại':
        return 'SURGERY'
    if group in ('Tiêu hóa', 'Cơ xương khớp', 'Thận – tiết niệu') and rng.random() < .40:
        return 'SURGERY'
    return 'INTERNAL_MEDICINE'


# Shares of *all diagnosis codes* within the two age strata in table 4.
# Codes outside this published subset keep their broader group label.
OUTPATIENT_CODES = {
    'Tiêu hóa': (('K29', 'Viêm dạ dày và tá tràng', 8.9, 3.4),
                 ('K76', 'Bệnh gan khác', 3.5, 2.8),
                 ('K64', 'Bệnh trĩ', 4.0, 2.5),
                 ('K21', 'Trào ngược dạ dày – thực quản', 4.0, None)),
    'Tim mạch': (('I10', 'Tăng huyết áp nguyên phát', 3.3, 12.0),),
    'Nội tiết': (('E78', 'Rối loạn chuyển hóa lipid', 6.2, 7.6),
                ('E11', 'Đái tháo đường týp 2', None, 5.6)),
    'Cơ xương khớp': (('M47', 'Thoái hóa cột sống', 2.5, None),
                      ('M17', 'Thoái hóa khớp gối', None, 2.5)),
}
# A blank table cell means the code was outside the top ten for that stratum.


def outpatient_diagnosis(patient, rng):
    group = patient['condition_group']
    if group not in OUTPATIENT_CODES:
        return {}
    senior = patient['age_years'] >= 60
    group_share = outpatient_weights(senior)[next(i for i, row in enumerate(OUTPATIENT) if row[0] == group)]
    draw = rng.uniform(0, group_share)
    for code, name, young, older in OUTPATIENT_CODES[group]:
        share = older if senior else young
        if share is not None:
            draw -= share
            if draw < 0:
                return dict(diagnosis_code=code, diagnosis_label=name)
    return {}
