const numeric = new Intl.NumberFormat('vi-VN', {maximumFractionDigits:1});
const vnDay = new Intl.DateTimeFormat('sv-SE', {timeZone:'Asia/Ho_Chi_Minh', year:'numeric', month:'2-digit', day:'2-digit'});
const watchKey = 'hospital-dashboard-visit';
const validId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,120}$/.test(value);
const number = value => value == null ? '—' : numeric.format(value);
const checkedAt = () => dateTime(new Date().toISOString());
let historyRows = null;
let selected = '';
let trackingVersion = 0;
let trackingRequest = null;
let loading = false;
const lastRead = new Map();

function renderFlow() {
  if (historyRows === null) return;
  const today = vnDay.format(new Date());
  const days = Number($('range').value);
  const first = new Date(today+'T00:00:00Z');
  first.setUTCDate(first.getUTCDate() - days + 1);
  const rows = new Map(historyRows.map(row => [row.date, row]));
  const visible = historyRows.filter(row => row.date >= first.toISOString().slice(0,10) && row.date <= today);
  const max = Math.max(1, ...visible.flatMap(row => [row.admissions, row.discharges]));
  const lines = [];
  for (let i=0; i<days; i++, first.setUTCDate(first.getUTCDate()+1)) {
    const key = first.toISOString().slice(0,10);
    const line = el('div', undefined, 'flow-row');
    const day = el('time', key.slice(8)+'/'+key.slice(5,7));
    day.dateTime = key;
    line.append(day);
    const row = rows.get(key);
    if (row) {
      const pair = el('div', undefined, 'flow-pair');
      for (const [label, value] of [['Nhập viện',row.admissions], ['Xuất viện',row.discharges]]) {
        const bar = el('div', undefined, 'flow-track');
        const fill = el('b');
        fill.style.width = (84*value/max)+'%';
        const count = el('span',number(value));
        const accessibleLabel = el('span', label+' ', 'sr-label');
        count.prepend(accessibleLabel);
        bar.append(fill, count); pair.append(bar);
      }
      line.append(pair);
    } else line.append(el('span','Chưa ghi nhận dữ liệu','no-data'));
    lines.push(line);
  }
  $('flow').replaceChildren(...lines);
}
let chartDay = '';
let chosenHour = null;
const ageGroups = {child:'Dưới 18 tuổi',adult:'18–59 tuổi',senior:'Từ 60 tuổi',unknown:'Chưa ghi nhận'};
const waitGroups = {short:'Dưới 15 phút',medium:'15 đến dưới 30 phút',long:'30 đến dưới 60 phút',hour_plus:'Từ 60 phút',unknown:'Chưa ghi nhận'};

function distribution(target, entries, total, emptyLabel) {
  const rows = entries.map(([label, count]) => {
    const row = el('div', undefined, 'distribution-row');
    const caption = el('div', undefined, 'distribution-caption');
    const percent = 100 * count / total;
    caption.append(el('span', label), el('strong', number(count)+' lượt · '+number(percent)+'%'));
    const track = el('div', undefined, 'distribution-track');
    const bar = el('b');
    bar.style.width = percent+'%';
    track.append(bar);
    row.append(caption, track);
    return row;
  });
  target.replaceChildren(...(total ? rows : [el('p',emptyLabel,'muted small')]));
}

function showHours(hours) {
  const peak = Math.max(1,...hours.flatMap(h => [h.arrivals,h.admissions]));
  function inspect(hour) {
    chosenHour = hour;
    const metric = hours[hour];
    $('hour-detail').textContent = hour+'h–'+(hour+1)+'h · '+number(metric.arrivals)+' lượt đến · '+number(metric.admissions)+' lượt nhập viện';
    for (const cell of $('hour-chart').querySelectorAll('button')) {
      cell.setAttribute('aria-pressed',String(Number(cell.value)===hour));
    }
  }
  const focused = $('hour-chart').contains(document.activeElement) ? document.activeElement.value : null;
  const cells = Array.from({length:24}, (_, hour) => {
    const metric = hours[hour];
    if (!metric) return el('span',undefined,'future-hour');
    const cell = el('button');
    cell.type = 'button'; cell.value = hour;
    cell.title = hour+'h · '+number(metric.arrivals)+' lượt đến · '+number(metric.admissions)+' lượt nhập viện';
    cell.setAttribute('aria-label',cell.title);
    cell.onclick = () => inspect(hour);
    for (const count of [metric.arrivals,metric.admissions]) {
      const bar = el('i'); bar.style.height = (100*count/peak)+'%';
      cell.append(bar);
    }
    return cell;
  });
  $('hour-chart').replaceChildren(...cells);
  inspect(chosenHour ?? hours.length-1);
  if (focused !== null && cells[Number(focused)]) cells[Number(focused)].focus({preventScroll:true});
}
function renderVisitCharts(charts) {
  const note = $('charts-note');
  note.classList.toggle('stale', !charts);
  if (!charts) {
    note.textContent = chartDay ? 'Chưa cập nhật được. '+note.dataset.last : 'Chưa tải được số liệu biểu đồ.';
    return;
  }
  const day = vnDay.format(new Date(charts.as_of));
  if (chartDay !== day) chosenHour = null;
  chartDay = day;
  showHours(charts.hours);
  for (const [id, labels, counts, empty] of [
    ['age-chart',ageGroups,charts.ages,'Chưa ghi nhận lượt đăng ký.'],
    ['wait-chart',waitGroups,charts.waits,'Chưa ghi nhận lượt bắt đầu khám.']
  ]) {
    const total = Object.values(counts).reduce((a,b) => a+b,0);
    distribution($(id), Object.entries(labels).map(([key,name])=>[name,counts[key] || 0]),total,empty);
  }
  const conditions = Object.entries(charts.conditions).sort((a,b)=>b[1]-a[1] || a[0].localeCompare(b[0],'vi'));
  const total = conditions.reduce((n,row)=>n+row[1],0);
  distribution($('condition-chart'),conditions.slice(0,5),total,'Chưa ghi nhận lượt đăng ký.');
  $('more-conditions').hidden = conditions.length<=5;
  if (conditions.length>5) distribution($('condition-rest'),conditions.slice(5),total,'');
  note.dataset.last = 'Sự kiện đã xử lý đến lần truy vấn '+dateTime(charts.as_of)+'.';
  note.textContent = note.dataset.last + ' Các biểu đồ dùng chung bản số liệu tối đa 90 giây.';
}
function renderOverview(data) {
  for (const key of ['arrivals','admissions','discharges']) $(key).textContent = number(data.today[key]);
  renderVisitCharts(data.charts);
  const state = data.state;
  $('current').textContent = number(state?.current_patients);
  $('processed').textContent = number(state?.processed_events);
  $('latency').textContent = state?.latency_p95_seconds == null ? '—' : number(state.latency_p95_seconds)+' giây';
  $('published').textContent = dateTime(state?.event_time);
  return 'Số liệu ngày '+data.date.split('-').reverse().join('/')+' · Truy vấn '+dateTime(data.queried_at);
}
function renderStatus(data) {
  $('enabled').textContent = data.simulation.enabled ? 'Đang bật' : 'Đang tạm dừng';
  $('sent').textContent = number(data.simulation.sent_events)+' sự kiện';
  $('sent-at').textContent = dateTime(data.simulation.last_sent_at);
  return data.producer_error || 'Đọc trạng thái bộ gửi lúc '+checkedAt();
}
function renderRecent(data) {
  const rows = data.items.slice(0,6).map(row => {
    const card = el('div',undefined,'recent-row'), info = el('div');
    info.append(el('strong',row.patient_id),el('small',(events[row.last_event] || row.last_event)+' · '+dateTime(row.event_time)));
    const button = el('button','Theo dõi','secondary');
    button.type = 'button'; button.setAttribute('aria-label','Theo dõi '+row.encounter_id);
    button.onclick = () => selectVisit(row.encounter_id);
    card.append(info,button); return card;
  });
  $('recent').replaceChildren(...rows);
  return rows.length ? 'Gần nhất theo thời điểm sự kiện · '+dateTime(data.as_of) : 'Chưa có lượt khám.';
}
async function readPanel(path, note, show) {
  try {
    const data = await api(path);
    const message = show(data);
    lastRead.set(note, checkedAt());
    $(note).textContent = message;
    $(note).classList.toggle('stale', Boolean(data.producer_error));
  } catch {
    if (note === 'overview-note') renderVisitCharts(null);
    $(note).classList.add('stale');
    $(note).textContent = 'Chưa tải được dữ liệu.'+(lastRead.has(note) ? ' Lần đọc thành công gần nhất '+lastRead.get(note) : '');
  }
}
function selectVisit(id) {
  if (id && !validId(id)) return;
  selected = id;
  trackingVersion++;
  $('visit').value = id;
  $('tracked-id').textContent = id;
  $('track-tools').hidden = !id;
  $('tracked-events').replaceChildren();
  $('tracking-note').classList.remove('stale');
  $('tracking-note').textContent = id ? 'Đang đọc hành trình…' : 'Nhập mã hoặc chọn một lượt gần đây.';
  try {
    if (id) localStorage.setItem(watchKey,id); else localStorage.removeItem(watchKey);
  } catch {}
  const url = new URL(location.href);
  if (id) url.searchParams.set('visit',id); else url.searchParams.delete('visit');
  history.replaceState(null,'',url);
  return refreshTracking();
}
async function refreshTracking() {
  const version = trackingVersion;
  if (!selected || trackingRequest === version) return;
  trackingRequest = version;
  try {
    const rows = await api('timeline?'+new URLSearchParams({encounter_id:selected}));
    if (version !== trackingVersion) return;
    renderTimeline($('tracked-events'), rows.slice().reverse());
    $('tracking-note').classList.remove('stale');
    $('tracking-note').textContent = rows.length ? 'Sự kiện mới nhất ở trên · Đọc lúc '+checkedAt() :
      'Chưa thấy sự kiện cho mã này. Hệ thống sẽ kiểm tra lại sau 30 giây.';
  } catch {
    if (version !== trackingVersion) return;
    $('tracking-note').classList.add('stale');
    $('tracking-note').textContent = 'Chưa cập nhật được hành trình. Hệ thống sẽ thử lại.';
  } finally {
    if (trackingRequest === version) trackingRequest = null;
  }
}
async function refresh() {
  if (loading || document.hidden) return;
  loading = true;
  $('refresh').disabled = true;
  try {
    await Promise.all([
      readPanel('overview?charts=1','overview-note',renderOverview),
      readPanel('status','status-note',renderStatus),
      readPanel('history','history-note', data => {
        historyRows = data; renderFlow(); return 'Ngày hiện tại tính đến lần đọc '+checkedAt();
      }),
      readPanel('patients?page=1','recent-note',renderRecent),
      refreshTracking()
    ]);
  } finally { loading = false; $('refresh').disabled = false; }
}
$('range').onchange = renderFlow;
$('track-form').onsubmit = event => { event.preventDefault(); selectVisit($('visit').value.trim()); };
$('untrack').onclick = () => selectVisit('');
$('refresh').onclick = refresh;
let initial = new URLSearchParams(location.search).get('visit');
if (!initial) { try { initial = localStorage.getItem(watchKey); } catch {} }
if (validId(initial)) selectVisit(initial);
refresh();
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
setInterval(refresh,30000);
