"""Fictional campus, using the simulator's physical room and bed identifiers."""
from datetime import datetime, timezone
from generators.stream_generator.provincial import HOSPITALS

WARD_LABELS = {
    "ICU": "Hồi sức tích cực", "EMERGENCY": "Nội trú cấp cứu", "CARDIOLOGY": "Tim mạch",
    "SURGERY": "Ngoại khoa", "INTERNAL_MEDICINE": "Nội tổng hợp",
}


def snapshot(profile, rows):
    hospital = HOSPITALS.get(profile)
    rooms = {}
    bed_rooms = {}

    def room(key, wing, name, capacity):
        rooms[key] = dict(id=key, wing=wing, name=name, capacity=capacity, beds=[], patients=[])

    if hospital:
        for i, (specialty, _) in enumerate(hospital.clinics, 1):
            room(f"OP-{i:02}", "Khám ngoại trú", specialty, 1)
        for i in range(1, hospital.emergency_rooms + 1):
            room(f"ED-{i}", "Khám cấp cứu", "Phòng cấp cứu", 1)
        for ward, beds in hospital.beds.items():
            beds_per_room = 2 if ward == 'ICU' else 4
            for first in range(1, beds + 1, beds_per_room):
                index = (first - 1) // beds_per_room + 1
                key = f"{ward}-R{index:03}"
                count = min(beds_per_room, beds - first + 1)
                room(key, WARD_LABELS[ward], f"Buồng {index:03}", count)
                for bed in range(first, first + count):
                    bed_id = f"{ward}-B{bed:03}"
                    rooms[key]['beds'].append(bed_id)
                    bed_rooms[bed_id] = key

    for encounter, patient, event, since, ward, consulting_room, bed, specialty, doctor in rows:
        consulting = event == 'CONSULTATION_STARTED'
        key = None
        if consulting and consulting_room and consulting_room.startswith(('ED-', 'OP-')):
            key = consulting_room
        elif event in ('BED_ASSIGNED', 'TRANSFER') and ward and bed and bed.startswith(ward + '-B'):
            key = bed_rooms.get(bed)
        if key not in rooms:
            # Earlier demos used opaque bed IDs or published admissions without a bed.
            # Keep those patients visible without inventing a physical location.
            key = 'UNMAPPED-' + (ward or 'OP')
            if key not in rooms:
                room(key, 'Chưa xác định phòng', WARD_LABELS.get(ward, 'Khám bệnh'), None)
        rooms[key]['patients'].append(dict(encounter=encounter, patient=patient, since=since, bed=bed,
                                          specialty=specialty or WARD_LABELS.get(ward), doctor=doctor,
                                          status='Đang khám' if consulting else 'Đã nhập viện'))
    return dict(profile=profile, as_of=datetime.now(timezone.utc).isoformat(), rooms=list(rooms.values()))
