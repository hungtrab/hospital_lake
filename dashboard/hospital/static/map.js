const roomsPerFloor = 12;
let groups = new Map(), wing = '', floor = 0, selected = '', loading = false;

const matchSize = 6;
let matchPage = 0;
const linkedQuery = new URLSearchParams(location.search).get('q') || '';
$('map-query').value = linkedQuery.slice(0,120);
let locateOnLoad = Boolean(linkedQuery);

function normalized(value) {
  return String(value || '').toLocaleLowerCase('vi').normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '').replace(/đ/g, 'd');
}

function matchedPatient(room, tokens) {
  const common = normalized([room.id, room.name, room.wing].join(' '));
  const matches = text => tokens.every(token => text.includes(token));
  if (matches(common)) return {label: ''};
  for (const person of room.patients) {
    const text = [person.patient, person.encounter, person.bed, person.doctor, person.specialty, person.status].join(' ');
    if (matches(common + ' ' + normalized(text))) return {label:person.patient};
  }
  for (const bed of room.beds || []) {
    if (matches(common + ' ' + normalized(bed))) return {label:bed};
  }
  return null;
}

function locate(room) {
  wing = room.wing;
  floor = Math.floor(groups.get(wing).indexOf(room) / roomsPerFloor);
  selected = room.id;
  draw();
  $('room-title').scrollIntoView({block:'nearest'});
}

function drawMatches() {
  const tokens = normalized($('map-query').value.trim()).split(/\s+/).filter(Boolean);
  const zone = $('filter-wing').value;
  const occupancy = $('filter-occupancy').value;
  const active = Boolean(tokens.length || zone || occupancy);
  $('map-matches').hidden = !active;
  const matches = [];
  if (active) for (const [area, rooms] of groups) {
    if (zone && area !== zone) continue;
    rooms.forEach((room, index) => {
      const n = room.patients.length;
      if (occupancy === 'empty' && n || occupancy === 'occupied' && !n) return;
      if (occupancy === 'full' && (room.capacity == null || n < room.capacity)) return;
      if (occupancy === 'available' && (room.capacity == null || n >= room.capacity)) return;
      const person = matchedPatient(room, tokens);
      if (person) matches.push({room, floor:1 + Math.floor(index / roomsPerFloor), label:person.label});
    });
  }
  const ids = new Set(matches.map(m => m.room.id));
  for (const tile of $('plan').querySelectorAll('[data-room]')) {
    tile.classList.toggle('unmatched', active && !ids.has(tile.dataset.room));
  }
  if (!active) return;
  if (locateOnLoad && groups.size) {
    locateOnLoad = false;
    if (matches.length === 1 && selected !== matches[0].room.id) {
      locate(matches[0].room);
      return;
    }
  }
  matchPage = Math.min(matchPage, Math.max(0, Math.ceil(matches.length / matchSize) - 1));
  const start = matchPage * matchSize;
  $('map-count').textContent = matches.length
    ? (start + 1) + '–' + Math.min(start + matchSize, matches.length) + ' / ' + matches.length + ' phòng phù hợp'
    : 'Không tìm thấy phòng phù hợp.';
  $('prev-room').disabled = !matchPage;
  $('next-room').disabled = start + matchSize >= matches.length;
  $('room-results').replaceChildren();
  for (const match of matches.slice(start, start + matchSize)) {
    const r = match.room;
    const button = el('button', r.id.startsWith('UNMAPPED-') ? r.name : r.id, 'room-result');
    button.dataset.result = r.id;
    button.append(el('small', [r.wing, 'Tầng ' + match.floor, r.patients.length + ' người', match.label].filter(Boolean).join(' · ')));
    button.onclick = () => locate(r);
    $('room-results').append(button);
  }
}

function total(rooms) {
  return rooms.reduce((count, room) => count + room.patients.length, 0);
}

function chooseWing(name) {
  wing = name;
  const rooms = groups.get(wing);
  const first = Math.max(0, rooms.findIndex(room => room.patients.length));
  floor = Math.floor(first / roomsPerFloor);
  selected = rooms[first].id;
  draw();
}

function draw() {
  const rooms = groups.get(wing);
  if (!rooms?.length) return;
  $('wings').replaceChildren();
  for (const [name, block] of groups) {
    const button = el('button', name);
    button.append(el('span', String(total(block))));
    button.setAttribute('aria-label', name + ', ' + total(block) + ' người');
    button.setAttribute('aria-pressed', name === wing);
    button.onclick = () => chooseWing(name);
    $('wings').append(button);
  }
  $('wing-name').textContent = wing;
  $('wing-summary').textContent = rooms.length + ' phòng hoặc khu vực · ' + total(rooms) + ' người';
  $('floor').replaceChildren();
  const floors = Math.ceil(rooms.length / roomsPerFloor);
  floor = Math.min(floor, floors - 1);
  for (let n = 0; n < floors; n++) {
    const count = total(rooms.slice(n * roomsPerFloor, (n + 1) * roomsPerFloor));
    const option = new Option((n + 1) + ' · ' + count + ' người', String(n));
    option.selected = n === floor;
    $('floor').add(option);
  }
  const visible = rooms.slice(floor * roomsPerFloor, (floor + 1) * roomsPerFloor);
  const chosen = visible.find(room => room.id === selected) || visible[0];
  selected = chosen.id;
  const banks = [el('div', undefined, 'room-bank'), el('div', undefined, 'room-bank')];
  visible.forEach((room, i) => {
    const label = room.id.startsWith('UNMAPPED-') ? room.name : room.id.includes('-R') ? room.name : room.id;
    const tile = el('button', label, 'room-tile' + (room.patients.length ? ' occupied' : ''));
    if (room.id.startsWith('OP-')) tile.append(el('span', room.name));
    tile.append(el('span', room.patients.length + (room.capacity == null ? '' : ' / ' + room.capacity) + ' người'));
    tile.title = room.name; tile.dataset.room = room.id;
    tile.setAttribute('aria-pressed', selected === room.id);
    tile.onclick = () => {
      selected = room.id;
      draw();
      document.querySelector('.room-tile[aria-pressed=true]')?.focus({preventScroll:true});
      if (matchMedia('(max-width:680px)').matches) $('room-title').scrollIntoView({block:'center'});
    };
    banks[i < Math.ceil(visible.length / 2) ? 0 : 1].append(tile);
  });
  $('plan').replaceChildren(banks[0], el('div', 'Hành lang · Tầng ' + (floor + 1), 'hall'), banks[1]);
  $('room-title').textContent = chosen.name;
  $('room-meta').textContent = (chosen.id.startsWith('UNMAPPED-') ? 'Chưa có mã phòng trên bản đồ' : chosen.id) + ' · ' + chosen.patients.length + ' người';
  $('occupants').replaceChildren();
  if (!chosen.patients.length) $('occupants').append(el('p', 'Phòng đang trống.'));
  if (chosen.beds?.length) {
    const beds = el('div', undefined, 'bed-list');
    for (const id of chosen.beds) {
      const occupied = chosen.patients.some(person => person.bed === id);
      beds.append(el('span', id + (occupied ? ' · Có người' : ' · Trống'), occupied ? 'bed-used' : ''));
    }
    $('occupants').append(beds);
  }
  for (const p of chosen.patients) {
    const card = el('article', undefined, 'occupant');
    card.append(el('strong', p.patient), el('p', p.status + ' · ' + dateTime(p.since)), el('p', 'Lượt ' + p.encounter));
    if (p.bed) card.append(el('p', 'Giường ' + p.bed));
    if (p.specialty) card.append(el('p', p.specialty));
    if (p.doctor) card.append(el('p', 'Bác sĩ ' + p.doctor));
    const button = el('button', 'Xem hành trình');
    button.onclick = () => timeline(p.encounter);
    card.append(button); $('occupants').append(card);
  }
  drawMatches();
}

$('floor').onchange = e => { floor = Number(e.target.value); selected = ''; draw(); };
async function refreshMap() {
  if (loading || document.hidden) return;
  loading = true;
  try {
    const data = await api('rooms');
    const next = new Map();
    for (const room of data.rooms) {
      if (!next.has(room.wing)) next.set(room.wing, []);
      next.get(room.wing).push(room);
    }
    groups = next;
    const selector = $('filter-wing');
    const options = [...selector.options].slice(1).map(option => option.value);
    if (options.join('|') !== [...groups.keys()].join('|')) {
      const previous = selector.value;
      selector.replaceChildren(new Option('Tất cả khu',''));
      for (const name of groups.keys()) selector.add(new Option(name, name));
      selector.value = groups.has(previous) ? previous : '';
    }
    if (!groups.has(wing)) {
      const preferred = [...groups].find(([name, rooms]) => !name.startsWith('Chưa') && total(rooms)) || [...groups][0];
      if (preferred) chooseWing(preferred[0]);
    } else draw();
    $('map-stamp').textContent = 'Cập nhật ' + dateTime(data.as_of) + ' · Tự làm mới mỗi 20 giây';
  } catch (err) {
    $('map-stamp').textContent = 'Chưa lấy được dữ liệu mới. ' + (groups.size ? 'Đang hiển thị lần cập nhật trước.' : err.message);
  } finally { loading = false; }
}
$('map-filters').addEventListener('input', () => { locateOnLoad = false; matchPage = 0; drawMatches(); });
$('map-filters').addEventListener('submit', e => { e.preventDefault(); drawMatches(); });
$('map-filters').addEventListener('reset', () => {
  locateOnLoad = false;
  matchPage = 0;
  setTimeout(drawMatches, 0);
});
$('prev-room').onclick = () => { matchPage--; drawMatches(); };
$('next-room').onclick = () => { matchPage++; drawMatches(); };
refreshMap();
setInterval(refreshMap, 20000);
document.addEventListener('visibilitychange', refreshMap);
