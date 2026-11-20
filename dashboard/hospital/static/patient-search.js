const patientForm = $('patient-filters');
let patientPage = 1;
let patientSnapshot = null;
let requestVersion = 0;
let typingTimer;

for (const [key, label] of Object.entries(visitStates)) $('patient-status').add(new Option(label, key));
for (const [key, label] of Object.entries(departments)) $('patient-department').add(new Option(label, key));

function displayVisits(rows) {
  const body = $('patient-rows');
  body.replaceChildren();
  if (!rows.length) return empty(body, 'Không có lượt khám phù hợp.', 5);
  for (const visit of rows) {
    const tr = el('tr');
    const id = el('td', visit.patient_id.replace('P-VISIT-', 'BN-'), 'code');
    id.append(el('small', visit.encounter_id, 'sub-id'));
    tr.append(id, el('td', departments[visit.department] || visit.department || 'Khu khám bệnh'));
    const state = el('td');
    state.append(el('span', visitStates[visit.status], 'badge' + (visit.status === 'discharged' ? ' closed' : '')));
    const actions = el('td');
    const button = el('button', 'Xem');
    button.onclick = () => timeline(visit.encounter_id);
    actions.append(button);
    if (['consulting','inpatient'].includes(visit.status)) {
      const link = el('a', 'Tìm phòng', 'locate');
      link.href = '/map.html?' + new URLSearchParams({q:visit.encounter_id});
      actions.append(link);
    }
    tr.append(state, el('td', dateTime(visit.event_time)), actions);
    body.append(tr);
  }
}

async function loadPatients(useSnapshot = false) {
  const version = ++requestVersion;
  const params = new URLSearchParams({page: patientPage});
  if (useSnapshot && patientSnapshot) params.set('as_of', patientSnapshot);
  for (const [key, value] of new FormData(patientForm)) {
    if (value.trim()) params.set(key, value.trim());
  }
  $('previous-patients').disabled = true;
  $('next-patients').disabled = true;
  if (params.has('from') && params.has('to') && params.get('from') > params.get('to')) {
    $('patient-rows').replaceChildren();
    $('patient-count').textContent = 'Ngày kết thúc phải từ ngày bắt đầu trở đi.';
    return;
  }
  try {
    const result = await api('patients?' + params);
    if (requestVersion !== version) return;
    patientSnapshot = result.as_of;
    displayVisits(result.items);
    $('patient-count').textContent = 'Trang ' + patientPage + ' · ' + result.items.length + ' lượt khám';
    $('previous-patients').disabled = patientPage === 1;
    $('next-patients').disabled = !result.more;
  } catch (err) {
    if (requestVersion !== version) return;
    empty($('patient-rows'), 'Không tải được kết quả.', 5);
    $('patient-count').textContent = err.message;
    $('previous-patients').disabled = patientPage === 1;
  }
}

function changePatientFilters(delay = 0) {
  clearTimeout(typingTimer);
  requestVersion++;
  patientPage = 1;
  patientSnapshot = null;
  $('patient-count').textContent = 'Đang tìm…';
  $('patient-rows').replaceChildren();
  $('previous-patients').disabled = $('next-patients').disabled = true;
  typingTimer = setTimeout(loadPatients, delay);
}
patientForm.addEventListener('input', e => changePatientFilters(e.target.type === 'search' ? 300 : 0));
patientForm.addEventListener('submit', e => { e.preventDefault(); changePatientFilters(); });
patientForm.addEventListener('reset', () => changePatientFilters());
$('previous-patients').onclick = () => { patientPage--; loadPatients(true); };
$('next-patients').onclick = () => { patientPage++; loadPatients(true); };
loadPatients();
setInterval(() => { if (!document.hidden && patientPage === 1) loadPatients(); }, 20000);
