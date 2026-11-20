const $ = id => document.getElementById(id);
const departments = {ICU:'Hồi sức tích cực',CARDIOLOGY:'Tim mạch',INTERNAL_MEDICINE:'Nội tổng hợp',SURGERY:'Ngoại khoa',EMERGENCY:'Cấp cứu'};
const events = {CONSULTATION_STARTED:'Vào phòng khám',OUTPATIENT_COMPLETED:'Hoàn tất ngoại trú',PATIENT_REGISTERED:'Đăng ký',ADMISSION:'Nhập viện',BED_ASSIGNED:'Nhận giường',TRANSFER:'Chuyển khoa',LAB_RESULT:'Kết quả xét nghiệm',VITAL_MEASUREMENT:'Đo chỉ số',BED_RELEASED:'Trả giường',DISCHARGE:'Xuất viện'};
function dateTime(s) { if (!s) return '—'; const iso=s.replace(' ','T'); return new Date(/[Z]|[+-]\d\d:\d\d$/.test(iso)?iso:iso+'Z').toLocaleString('vi-VN',{timeZone:'Asia/Ho_Chi_Minh'}); }
function el(tag,text,cls) { const node=document.createElement(tag); if(text!==undefined)node.textContent=text; if(cls)node.className=cls; return node; }
async function api(path,options={}) { const response=await fetch('/api/'+path,{...options,headers:{'Content-Type':'application/json',...(options.headers||{})},signal:AbortSignal.timeout(35000)}); const result=await response.json(); if(!response.ok)throw new Error(result.error||'Không lấy được dữ liệu.'); return result; }
function renderTimeline(list, rows) {
  const items = rows.map(row => {
    const item = el('li');
    item.append(el('strong', events[row.event_type] || row.event_type));
    const context = [dateTime(row.event_time), departments[row.department] || row.department,
      row.is_abnormal ? 'Ngoài khoảng tham chiếu' : null];
    item.append(el('span', context.filter(Boolean).join(' · ')));
    if (row.event_type === 'PATIENT_REGISTERED') {
      const patient = [row.age_years != null ? row.age_years + ' tuổi' : null, row.condition_group,
        row.diagnosis_code ? row.diagnosis_code + ' · ' + row.diagnosis_label : null].filter(Boolean);
      if (patient.length) item.append(el('small', patient.join(' · ')));
    }
    if (row.specialty) item.append(el('small', [row.specialty, row.room, row.doctor, row.nurse,
      row.wait_seconds == null ? null : 'Chờ ' + Math.round(Number(row.wait_seconds)/60) + ' phút'].filter(Boolean).join(' · ')));
    return item;
  });
  list.replaceChildren(...(items.length ? items : [el('li','Chưa có sự kiện.')]));
}
let openJourney = 0;
async function timeline(id) {
  const request = ++openJourney;
  $('journey-id').textContent = id;
  $('track-journey').href = '/dashboard.html?' + new URLSearchParams({visit:id}) + '#tracking';
  $('events').replaceChildren(el('li','Đang tải…'));
  $('journey').showModal();
  try {
    const rows = await api('timeline?' + new URLSearchParams({encounter_id:id}));
    if (request === openJourney) renderTimeline($('events'), rows);
  } catch (err) {
    if (request === openJourney) $('events').replaceChildren(el('li',err.message));
  }
}
$('close-journey')?.addEventListener('click', () => $('journey').close());
