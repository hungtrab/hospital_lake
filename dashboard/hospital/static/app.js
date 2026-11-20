const visitStates = {discharged:'Đã xuất viện',inpatient:'Đang nằm viện',completed:'Đã khám xong',consulting:'Đang khám',waiting:'Chờ khám'};
let status = null, refreshing = false;
const number = n => Number(n).toLocaleString('vi-VN');
function empty(node,text,columns) { node.replaceChildren(); if(columns){const tr=el('tr');const td=el('td',text);td.colSpan=columns;tr.append(td);node.append(tr);}else node.append(el('p',text,'muted')); }
function showStatus(data) {
  status = data.simulation;
  const today = new Intl.DateTimeFormat('sv-SE', {timeZone:'Asia/Ho_Chi_Minh'}).format(new Date());
  const plan = status.days.find(day => day.day === today);
  const pilot = plan && today < status.start_date;
  $('sim-state').textContent = status.enabled
    ? (pilot ? 'Đang chạy thử' : 'Mô phỏng đã bật')
    : 'Mô phỏng đang tạm dừng';
  const details = [`Trung bình ${status.visits} lượt/ngày`, `đã gửi ${number(status.sent_events)} sự kiện`];
  if (status.profile === 'bach-mai-v1' && status.version >= 5) details.push('Kịch bản cuối tuần đông');
  if (pilot) details.unshift(`${plan.visits} lượt thử hôm nay`);
  $('sim-detail').textContent = details.join(' · ');
  $('toggle').disabled = false;
  $('toggle').textContent = status.enabled ? 'Tạm dừng mô phỏng' : 'Bật mô phỏng';
  if (data.producer_error) {
    $('notice').textContent = data.producer_error;
    $('notice').className = 'notice error';
  }
}
async function refresh(){if(refreshing)return;refreshing=true;const results=await Promise.allSettled(['status','overview','history','weekly'].map(path=>api(path)));let error=false;results.forEach((r,i)=>{if(r.status==='rejected'){error=true;if(i===3)empty($('weekly'),'Báo cáo lịch sử chưa sẵn sàng.',4);return;}const data=r.value;if(i===0)showStatus(data);if(i===1){$('arrivals').textContent=number(data.today.arrivals);$('outpatients').textContent=number(data.today.outpatients);$('current').textContent=data.state?number(data.state.current_patients):'—';$('admissions').textContent=number(data.today.admissions);$('discharges').textContent=number(data.today.discharges);$('icu').textContent=data.state?number(data.state.icu_occupancy):'—';$('icu-capacity').textContent=data.state?`Trên ${data.state.total_icu_beds} giường ICU`:'Chưa nhận sự kiện';$('business-date').textContent=data.date.split('-').reverse().join('/');$('labs').textContent=number(data.today.labs);$('abnormal').textContent=number(data.today.abnormal_labs);$('departments').replaceChildren();if(!data.departments.length)empty($('departments'),'Hiện chưa có người bệnh đang nằm viện.');for(const row of data.departments){const node=el('div',undefined,'department-row');node.append(el('span',departments[row.department]||row.department),el('strong',number(row.patients)+' người'));$('departments').append(node);}$('updated').textContent='Dữ liệu lúc '+dateTime(data.queried_at);}if(i===2){$('history').replaceChildren();if(!data.length)empty($('history'),'Lịch sử sẽ xuất hiện khi có lượt nhập viện.');const max=Math.max(1,...data.flatMap(r=>[r.admissions,r.discharges]));for(const row of data.slice().reverse()){const node=el('div',undefined,'history-row'),bars=el('div',undefined,'bars');for(const [key,cls] of [['admissions','bar'],['discharges','bar out']]){const bar=el('div',undefined,cls);bar.style.width=(100*row[key]/max)+'%';bars.append(bar);}node.append(el('span',row.date.slice(5).split('-').reverse().join('/')),bars,el('strong',row.admissions+' / '+row.discharges));$('history').append(node);}}if(i===3){$('weekly').replaceChildren();if(!data.length)empty($('weekly'),'Chưa đủ dữ liệu lịch sử trong hai tuần.',4);for(const row of data){const tr=el('tr');tr.append(el('td',departments[row.department]||row.department),el('td',number(row.previous_admissions)),el('td',number(row.current_admissions)),el('td',row.change_pct===null?'—':(row.change_pct>0?'+':'')+row.change_pct+'%'));$('weekly').append(tr);}}});$('notice').className='notice'+(error?' error':'');$('notice').textContent=error?'Một số nguồn chưa sẵn sàng. Số liệu cũ (nếu có) vẫn giữ mốc thời gian lần lấy thành công.':'Dữ liệu tổng hợp được lưu và xử lý qua pipeline thật. Trang tự cập nhật mỗi 20 giây.';if(results[0].status==='fulfilled' && results[0].value.producer_error){$('notice').textContent=results[0].value.producer_error;$('notice').className='notice error';}refreshing=false;}

$('toggle').addEventListener('click',async()=>{if(!status)return;$('toggle').disabled=true;try{await api('simulation',{method:'POST',body:JSON.stringify({enabled:!status.enabled})});await refresh();}catch(e){$('notice').className='notice error';$('notice').textContent=e.message;}finally{$('toggle').disabled=false;}});
function clock(){$('clock').textContent=new Date().toLocaleString('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',dateStyle:'full',timeStyle:'short'});}clock();setInterval(clock,30000);refresh();setInterval(()=>{if(!document.hidden)refresh();},20000);
