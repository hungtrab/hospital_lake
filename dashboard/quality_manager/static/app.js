"use strict";
const $ = id => document.getElementById(id);
const state = {runs: [], job: null, csrf: "", selected: null, report: null, reviews: {}, view: "overview", detail: null, polling: false};
const entities = {patients: "Bệnh nhân", encounters: "Lượt khám", lab_results: "Xét nghiệm", medications: "Thuốc", vitals: "Chỉ số sinh tồn", bed_events: "Sự kiện giường"};
const reviews = {pending: "Chưa rà soát", reviewed: "Đã rà soát", investigate: "Cần kiểm tra thêm"};
const statuses = {passed: "Kiểm tra đạt", failed: "Không đạt", incomplete: "Chưa hoàn tất", invalid: "Báo cáo lỗi", running: "Đang chạy", interrupted: "Bị gián đoạn"};
const reasons = {
  DUPLICATE_KEY: "Cùng mã nhưng thông tin mâu thuẫn", UNKNOWN_PATIENT: "Chưa có bệnh nhân hợp lệ", UNKNOWN_ENCOUNTER: "Chưa có lượt khám hợp lệ",
  UNKNOWN_DEPARTMENT: "Tên khoa chưa được nhận diện", INVALID_LAB_VALUE: "Số đo xét nghiệm không hợp lệ", INVALID_REFERENCE_RANGE: "Khoảng tham chiếu không hợp lệ",
  UNSUPPORTED_LAB_UNIT: "Đơn vị xét nghiệm chưa được hỗ trợ", LAB_TEST_NAME_MISMATCH: "Mã và tên xét nghiệm không khớp", UNKNOWN_LAB_TEST: "Mã xét nghiệm chưa được hỗ trợ",
  ENCOUNTER_PATIENT_MISMATCH: "Bệnh nhân không khớp lượt khám", DISCHARGE_BEFORE_ADMISSION: "Xuất viện trước khi nhập viện", STATUS_DISCHARGE_MISMATCH: "Trạng thái và ngày xuất viện không khớp",
  IMPLAUSIBLE_BIRTH_YEAR: "Năm sinh không hợp lý", INVALID_DOSE: "Liều thuốc không hợp lệ", END_BEFORE_START: "Kết thúc trước khi bắt đầu",
  INVALID_SPO2: "SpO2 nằm ngoài khoảng cho phép", INVALID_TEMPERATURE: "Nhiệt độ thiếu hoặc không hợp lệ", INVALID_BED_EVENT_TYPE: "Loại sự kiện giường không hợp lệ",
  MISSING_PATIENT_ID: "Thiếu mã bệnh nhân", MISSING_ADMISSION_TIME: "Thiếu thời điểm nhập viện"
};
function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === "class") node.className = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const child of children.flat()) if (child !== null && child !== undefined) node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  return node;
}
function badge(text, color = "neutral") { return el("span", {class: `badge ${color}`}, text); }
function displayDate(value) {
  return new Intl.DateTimeFormat("vi-VN", {day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}).format(new Date(value));
}
function showError(message) { $("error").textContent = message; $("error").hidden = !message; }
let toastTimer;
function toast(message) { $("toast").textContent = message; $("toast").hidden = false; clearTimeout(toastTimer); toastTimer = setTimeout(() => {$("toast").hidden = true;}, 3500); }
async function api(path, data) {
  const options = data === undefined ? {} : {method:"POST",headers:{"Content-Type":"application/json", "X-CSRF-Token":state.csrf},body:JSON.stringify(data)};
  const response = await fetch(path, options);
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || "Không kết nối được với web quản lý.");
  return result;
}
function total(key) { return state.report.entities.reduce((sum, row) => sum + row[key], 0); }
function outcome(item) {
  if (item.quarantine.length) return {id:"quarantine", label:`Cách ly · ${item.quarantine.length} dòng`, color:"amber"};
  if (item.before.length > item.after.length) return {id:"duplicate", label:"Đã gộp bản sao", color:"neutral"};
  return {id:"accepted", label:"Được chấp nhận", color:"green"};
}
function bar(values, className = "stacked-bar") {
  const node = el("div", {class:className});
  const sum = values.reduce((a,b) => a+b, 0);
  values.forEach((value, index) => { if (value > 0) {
    const part = el("span", {class:`segment ${["accepted", "quarantine", "duplicate"][index]}`});
    part.style.width = `${value / sum * 100}%`; node.append(part);
  }});
  return node;
}
function changeView(view) {
  state.view = ["overview", "scenarios", "history"].includes(view) ? view : "overview";
  document.querySelectorAll(".nav").forEach(button => {
    button.classList.toggle("active", button.dataset.view === state.view);
    if (button.dataset.view === state.view) button.setAttribute("aria-current", "page"); else button.removeAttribute("aria-current");
  });
  const titles = {overview:["Chất lượng dữ liệu", "Hiểu dữ liệu đi vào. Kiểm soát dữ liệu đi ra."], scenarios:["Kịch bản & rà soát", "Đối chiếu từng trường dữ liệu và ghi lại điều cần kiểm tra."], history:["Lịch sử kiểm tra", "Theo dõi các lần chạy và mở lại kết quả đã lưu."]};
  $("page-title").textContent = titles[state.view][0]; $("page-description").textContent = titles[state.view][1];
  for (const name of ["overview", "scenarios", "history"]) $("view-" + name).hidden = name !== state.view;
  $("report-content").hidden = !state.report || state.view === "history";
  $("empty").hidden = !!state.report || state.view === "history";
}
function renderHistory() {
  $("history-rows").replaceChildren(...state.runs.map(run => el("tr", {},
    el("td", {}, el("strong", {}, run.id), el("small", {class:"muted"}, run.spark_version ? `Spark ${run.spark_version}` : "Chưa có báo cáo hoàn tất")),
    el("td", {}, displayDate(run.created_at)),
    el("td", {}, badge(statuses[run.status], run.status === "passed" ? "green" : run.status === "incomplete" ? "neutral" : "amber")),
    el("td", {}, run.scenario_count ?? "—"),
    el("td", {}, ["passed", "failed"].includes(run.status) ? el("button", {class:"text-button", onclick:() => {loadRun(run.id).catch(error => showError(error.message)); location.hash="overview";}}, "Mở báo cáo →") : "—")
  )));
  $("history-empty").hidden = state.runs.length > 0;
}
function renderRunSelect() {
  const available = state.runs.filter(run => ["passed", "failed"].includes(run.status));
  $("run-select").replaceChildren(...available.map(run => el("option", {value:run.id}, `${displayDate(run.created_at)} · ${run.id.slice(-5)}`)));
  if (!available.length) $("run-select").append(el("option", {}, "Chưa có báo cáo"));
  $("run-select").disabled = !available.length;
  if (state.selected) $("run-select").value = state.selected;
}
function renderJob() {
  const job = state.job;
  $("job-panel").hidden = !job;
  const running = job?.status === "running";
  $("run-button").disabled = running; $("empty-run").disabled = running;
  $("run-button").replaceChildren(el("span", {"aria-hidden":"true"}, running ? "◌" : "▶"), running ? "Đang kiểm tra…" : "Chạy bộ kiểm tra");
  if (!job) return;
  $("job-title").textContent = {running:"Đang chạy kiểm tra bằng Spark", passed:"Lượt kiểm tra đã hoàn tất", failed:"Lượt kiểm tra không thành công", interrupted:"Lượt kiểm tra trước bị gián đoạn"}[job.status];
  $("job-description").textContent = running ? "Đang tạo dữ liệu, làm sạch và kiểm chứng Gold. Bạn có thể tiếp tục xem báo cáo đã có." : job.status === "passed" ? "Báo cáo mới đã sẵn sàng trong lịch sử chạy." : "Mở nhật ký để xem nguyên nhân. Kết quả chưa hoàn tất không được tính là đạt.";
  $("job-log").textContent = job.log || job.error || "Đang khởi động…";
}
async function refresh() {
  if (state.polling) return;
  state.polling = true;
  try {
    const previous = state.job;
    const data = await api("/api/state");
    state.runs = data.runs; state.job = data.job; state.csrf = data.csrf_token;
    renderHistory(); renderRunSelect(); renderJob();
    if (previous?.status === "running" && state.job?.status === "passed" && state.job.run_id) {
      await loadRun(state.job.run_id); toast("Kiểm tra hoàn tất. Đã mở kết quả mới.");
    } else if (!state.selected) {
      const run = state.runs.find(row => row.status === "passed") || state.runs.find(row => row.status === "failed");
      if (run) await loadRun(run.id); else { $("run-badge").textContent = "Chưa có kết quả"; changeView(state.view); }
    }
  } catch (error) { showError(error.message); }
  finally { state.polling = false; }
}
async function loadRun(id) {
  const data = await api(`/api/runs/${encodeURIComponent(id)}`);
  state.selected = id; state.report = data.report; state.reviews = data.reviews;
  $("detail-dialog").close(); state.detail = null;
  showError(""); renderRunSelect(); renderReport(); changeView(state.view);
}
function renderReport() {
  const report = state.report;
  $("scenario-count").textContent = report.scenario_count;
  $("run-badge").textContent = report.passed ? "✓ Kiểm tra đạt" : "Không đạt";
  $("run-badge").className = `badge ${report.passed ? "green" : "amber"}`;
  $("download").hidden = false; $("download").href = `/api/runs/${state.selected}/download`;
  for (const [element,key] of [["total-input","bronze_rows"],["total-valid","output_rows"],["total-rejected","quarantine_rows"],["total-duplicates","exact_duplicate_rows"]]) $(element).textContent = total(key).toLocaleString("vi-VN");
  const amounts = [total("output_rows"), total("quarantine_rows"), total("exact_duplicate_rows")];
  const accounted = amounts.reduce((a,b)=>a+b,0) === total("bronze_rows");
  $("accounting").textContent = accounted ? "✓ Đối soát khớp" : "Đối soát chưa khớp";
  $("accounting").className = `badge ${accounted ? "green" : "amber"}`;
  $("distribution").replaceChildren(...bar(amounts).childNodes);
  $("legend").replaceChildren(...amounts.map((n,i)=>el("span", {}, el("span", {class:`legend-dot ${["accepted","quarantine","duplicate"][i]}`}), `${["Chấp nhận", "Cách ly", "Bản sao gộp"][i]} ${n}`)));
  $("checks-passed").textContent = report.scenarios.filter(row=>row.passed).length;
  $("checks-total").textContent = `/ ${report.scenario_count}`;
  $("rerun-check").textContent = report.entities.every(row=>row.rerun_identical) ? "✓ Chạy lại cho cùng kết quả" : "Chưa xác nhận chạy lại";
  $("entity-count").textContent = `${report.entities.length} bảng`;
  $("entity-rows").replaceChildren(...report.entities.map(row=>el("tr", {},
    el("td", {}, el("div", {class:"entity-name"}, el("span", {class:"entity-icon", "aria-hidden":"true"}, "▤"), el("div", {}, el("strong", {}, entities[row.entity] || row.entity), el("small", {}, row.entity)))),
    el("td", {}, row.bronze_rows), el("td", {}, badge(row.output_rows, "green")), el("td", {}, badge(row.quarantine_rows, row.quarantine_rows ? "amber" : "neutral")), el("td", {}, row.exact_duplicate_rows),
    el("td", {}, bar([row.output_rows,row.quarantine_rows,row.exact_duplicate_rows],"mini-bar"))
  )));
  const counts = {};
  for (const row of report.entities) for (const [code,count] of Object.entries(row.reason_counts || {})) counts[code] = (counts[code] || 0) + count;
  const common = Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,5);
  $("reason-list").replaceChildren(...common.map(([code,count],index)=>el("div", {class:"reason-item"}, el("span",{class:"reason-index"},String(index+1).padStart(2,"0")), el("span",{class:"label",title:code},reasons[code] || code), el("span",{class:"count"},count))));
  if (!common.length) $("reason-list").append(el("p",{class:"muted"},"Không có lý do cách ly trong lần chạy này."));
  const hourly = [...(report.gold.hospital_hourly_metrics || [])].sort((a,b)=>String(a.hour).localeCompare(String(b.hour)));
  const labs = report.gold.lab_daily_metrics || [];
  const tests = labs.reduce((sum,row)=>sum+row.total_tests,0), abnormal = labs.reduce((sum,row)=>sum+row.abnormal_tests,0);
  $("gold-metrics").replaceChildren(...[
    ["Lượt nhập viện",hourly.reduce((sum,row)=>sum+row.admissions,0)],
    ["Còn nằm viện tại thời điểm chốt",hourly.at(-1)?.current_patients ?? "—"],
    ["Xét nghiệm hợp lệ",tests],
    ["Tỷ lệ xét nghiệm bất thường", tests ? `${(abnormal/tests*100).toLocaleString("vi-VN",{maximumFractionDigits:1})}%` : "Chưa có dữ liệu"]
  ].map(([label,value])=>el("div",{class:"gold-item"},el("span",{},label),el("strong",{},value))));
  const previousFilter = $("entity-filter").value;
  $("entity-filter").replaceChildren(el("option",{value:""},"Tất cả bảng"),...report.entities.map(row=>el("option",{value:row.entity},entities[row.entity] || row.entity)));
  $("entity-filter").value = previousFilter;
  renderScenarios();
}
const fold = value => String(value).normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/đ/g,"d").toLowerCase();
function renderScenarios() {
  if (!state.report) return;
  const query = fold($("search").value.trim()), entity = $("entity-filter").value, result = $("outcome-filter").value, reviewed = $("review-filter").value;
  const cases = state.report.scenarios.filter(item=> {
    const codes = item.quarantine.flatMap(row=>row.reason_codes);
    const haystack = [item.id,item.description,item.entity,...codes,...codes.map(code=>reasons[code] || "")].join(" ");
    return (!query || fold(haystack).includes(query)) && (!entity || item.entity === entity) && (!result || outcome(item).id === result || (result === "accepted" && item.after.length > 0)) && (!reviewed || (state.reviews[item.id]?.status || "pending") === reviewed);
  });
  $("filtered-count").textContent = `${cases.length} / ${state.report.scenario_count} kịch bản`;
  $("scenario-rows").replaceChildren(...cases.map(item=> {
    const result = outcome(item), review = state.reviews[item.id]?.status || "pending";
    return el("tr", {}, el("td", {}, el("strong",{},item.id),el("p",{},item.description)), el("td",{class:"muted"},entities[item.entity] || item.entity), el("td",{},badge(result.label,result.color)), el("td",{},badge(reviews[review],review === "reviewed" ? "green" : review === "investigate" ? "amber" : "neutral")), el("td",{},el("button",{class:"text-button",onclick:()=>openDetail(item.id),"aria-label":`Xem chi tiết ${item.id}`},"Chi tiết →")));
  }));
  $("no-matches").hidden = cases.length > 0;
}
function valueText(value) { return value === undefined ? "Chưa có trường này" : value === null ? "null · chưa có giá trị" : typeof value === "object" ? JSON.stringify(value) : String(value); }
function openDetail(id) {
  const item = state.report.scenarios.find(row=>row.id===id); state.detail = id;
  $("detail-id").textContent = `${id} / ${entities[item.entity] || item.entity}`; $("detail-title").textContent = item.description;
  const result = outcome(item);
  $("detail-badges").replaceChildren(badge(result.label,result.color),badge(item.passed ? "Đúng kỳ vọng kiểm thử" : "Khác kỳ vọng",item.passed ? "green" : "amber"));
  const codes = [...new Set(item.quarantine.flatMap(row=>row.reason_codes))];
  $("detail-reasons").hidden = !codes.length;
  $("detail-reasons").replaceChildren(...codes.map(code=>el("div",{},reasons[code] || "Bản ghi cần được kiểm tra lại",el("code",{},code))));
  const fields = [...new Set([...item.before,...item.after].flatMap(row=>Object.keys(row)))].filter(key=>!key.startsWith("_"));
  $("diff-rows").replaceChildren(...fields.map(field=> {
    const before = [...new Set(item.before.map(row=>valueText(row[field])))], after = [...new Set(item.after.map(row=>valueText(row[field])))];
    const changed = after.length && JSON.stringify(before)!==JSON.stringify(after);
    return el("tr",{class:changed ? "changed" : ""},el("td",{},field),el("td",{},before.map(value=>el("div",{},value))),el("td",{},after.length ? after.map(value=>el("div",{},value)) : el("span",{class:"missing-value"},"Không đưa vào Silver")));
  }));
  $("raw-json").textContent = JSON.stringify({before:item.before,after:item.after,quarantine:item.quarantine},null,2);
  const review = state.reviews[id] || {status:"pending",note:""};
  $("review-status").value = review.status; $("review-note").value = review.note;
  $("review-feedback").textContent = review.updated_at ? `Đã lưu ${displayDate(review.updated_at)}` : "";
  $("detail-dialog").querySelector("details").open = false;
  $("detail-dialog").showModal();
  $("detail-dialog").scrollTop = 0;
}
async function startJob() {
  $("run-button").disabled = true; $("empty-run").disabled = true;
  showError("");
  try { state.job = await api("/api/jobs", {}); renderJob(); toast("Đã bắt đầu kiểm tra. Kết quả cũ vẫn được giữ."); }
  catch (error) { showError(error.message); await refresh(); }
}
$("review-form").addEventListener("submit",async event=> {
  event.preventDefault(); const button = event.target.querySelector("button"); button.disabled = true;
  try {
    const value = await api(`/api/runs/${state.selected}/reviews/${state.detail}`,{status:$("review-status").value,note:$("review-note").value});
    state.reviews[state.detail] = value; $("review-feedback").textContent = "Đã lưu rà soát."; renderScenarios(); toast("Đã lưu ghi chú và trạng thái rà soát.");
  } catch (error) { $("review-feedback").textContent = `Không lưu được: ${error.message}`; }
  finally { button.disabled = false; }
});
document.querySelectorAll(".nav").forEach(button=>button.addEventListener("click",()=>{location.hash=button.dataset.view;}));
window.addEventListener("hashchange",()=>changeView(location.hash.slice(1)));
$("see-scenarios").addEventListener("click",()=>{location.hash="scenarios";});
$("run-select").addEventListener("change",event=>loadRun(event.target.value).catch(error=>showError(error.message)));
for (const id of ["search","entity-filter","outcome-filter","review-filter"]) $(id).addEventListener("input",renderScenarios);
$("close-dialog").addEventListener("click",()=>$("detail-dialog").close());
$("run-button").addEventListener("click",startJob); $("empty-run").addEventListener("click",startJob);
$("refresh").addEventListener("click",async()=>{await refresh(); if(state.selected) await loadRun(state.selected).catch(error=>showError(error.message));});
$("log-toggle").addEventListener("click",()=>{$("job-log").hidden=!$("job-log").hidden; $("log-toggle").setAttribute("aria-expanded",String(!$("job-log").hidden)); $("log-toggle").textContent=$("job-log").hidden ? "Xem nhật ký" : "Ẩn nhật ký";});
changeView(location.hash.slice(1)); refresh(); setInterval(refresh,3000);
