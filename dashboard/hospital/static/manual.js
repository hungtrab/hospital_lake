let visits = [];
let wards = [];
let selectedId = null;
let busy = false;
let pending = null;
const recentKey = 'hospital-manual-command';

function notice(message, error = false) {
  $('message').textContent = message;
  $('message').classList.toggle('error', error);
}
function wardOptions(select, values) {
  const previous = select.value;
  select.replaceChildren(...values.map(value => new Option(departments[value] || value, value)));
  if (values.includes(previous)) select.value = previous;
}
function render() {
  const selected = visits.find(row => row.encounter_id === selectedId);
  $('cases').replaceChildren();
  for (const row of visits) {
    const tr = el('tr');
    for (const value of [row.patient_id, departments[row.department], row.bed_id || '—',
                         row.bed_id ? 'Đang nội trú' : 'Đã xuất viện', dateTime(row.updated_at)]) {
      tr.append(el('td', value));
    }
    const cell = el('td'), pick = el('button', row === selected ? 'Đang chọn' : 'Chọn');
    pick.disabled = busy || row === selected;
    pick.addEventListener('click', () => { selectedId = row.encounter_id; render(); });
    cell.append(pick); tr.append(cell); $('cases').append(tr);
  }
  if (!visits.length) {
    const cell = el('td', 'Chưa có lượt nhập tay.'); cell.colSpan = 6;
    const tr = el('tr'); tr.append(cell); $('cases').append(tr);
  }
  wardOptions($('ward'), wards);
  $('selected').textContent = selected ? [selected.patient_id, departments[selected.department],
    selected.bed_id || 'Đã xuất viện'].join(' · ') : 'Chọn một lượt bên dưới hoặc nhập viện để bắt đầu.';
  $('links').hidden = !selected;
  $('change').hidden = !selected?.bed_id;
  $('locate').hidden = !selected?.bed_id;
  if (selected) {
    $('locate').href = '/map.html?q=' + encodeURIComponent(selected.encounter_id);
    wardOptions($('destination'), wards.filter(ward => ward !== selected.department));
  }
  for (const field of document.querySelectorAll('#intake input, #intake select, #intake button, #change select, #change button, #reload')) {
    field.disabled = busy || (!wards.length && field.id !== 'reload');
  }
}
async function refresh() {
  const data = await api('manual');
  visits = data.items;
  wards = data.departments;
  const condition = $('condition').value;
  $('condition').replaceChildren(new Option('Chưa ghi nhận', ''), ...data.condition_groups.map(g => new Option(g,g)));
  $('condition').value = condition;
  render();
}
async function submit(body) {
  if (busy) return;
  busy = true;
  const signature = JSON.stringify(body);
  if (pending?.signature !== signature) pending = {signature, request_id:crypto.randomUUID()};
  try { sessionStorage.setItem(recentKey, JSON.stringify(pending)); } catch {}
  render();
  notice('Đang ghi nhận…');
  let saved;
  try {
    saved = await api('manual', {method:'POST', body:JSON.stringify({...body, request_id:pending.request_id})});
    pending = null;
    try { sessionStorage.removeItem(recentKey); } catch {}
    selectedId = saved.encounter_id;
    visits = [saved, ...visits.filter(row => row.encounter_id !== selectedId)];
    if (body.action === 'admit') $('intake').reset();
    render();
    const label = {admit:'Đã nhập viện', transfer:'Đã chuyển khoa', discharge:'Đã xuất viện'};
    notice(label[body.action] + ' · ' + saved.patient_id + (saved.bed_id ? ' · ' + saved.bed_id : ''));
  } catch (error) { notice(error.message, true); }
  try { await refresh(); }
  catch (error) { if (saved) notice('Đã lưu thao tác. Danh sách chưa tải lại được.', true); }
  finally { busy = false; render(); }
}
$('intake').addEventListener('submit', event => {
  event.preventDefault();
  const body = {action:'admit', code:$('code').value, department:$('ward').value};
  if ($('age').value !== '') body.age_years = $('age').valueAsNumber;
  if ($('condition').value) body.condition_group = $('condition').value;
  submit(body);
});
$('code').addEventListener('input', () => { $('code').value = $('code').value.toUpperCase(); });
function change(action) {
  const row = visits.find(item => item.encounter_id === selectedId);
  if (!row?.bed_id) return;
  if (action === 'discharge' && !window.confirm('Xuất viện và trả giường cho ' + row.patient_id + '?')) return;
  const body = {action, encounter_id:row.encounter_id, revision:row.revision};
  if (action === 'transfer') body.department = $('destination').value;
  submit(body);
}
$('change').addEventListener('submit', event => { event.preventDefault(); change('transfer'); });
$('discharge').addEventListener('click', () => change('discharge'));
$('show-events').addEventListener('click', () => timeline(selectedId));
async function loadVisits() {
  if (busy) return;
  busy = true;
  render();
  try { await refresh(); notice(wards.length ? 'Sẵn sàng nhập tay.' : 'Cấu hình hiện tại chưa có giường nội trú.'); }
  catch (error) { notice(error.message, true); }
  finally { busy = false; render(); }
}
$('reload').addEventListener('click', loadVisits);
try { pending = JSON.parse(sessionStorage.getItem(recentKey)); } catch { pending = null; }
loadVisits();
