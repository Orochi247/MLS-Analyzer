# mls_app.py - Full Flask prototype with Google Sheets symbol export
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, send_file
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import csv, io, os, base64, json

# Optional libs for Sheets; app will still run if they're not installed
try:
    import pandas as pd
    import gspread
    from gspread_dataframe import set_with_dataframe
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except Exception:
    GSPREAD_AVAILABLE = False

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///mls_cleanup.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- Models ---
class Listing(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    batch = db.Column(db.String(120), default='default')
    listing_id_text = db.Column(db.String(120), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Field(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    canonical = db.Column(db.String(200), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Observation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    listing_id = db.Column(db.Integer, db.ForeignKey('listing.id'), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey('field.id'), nullable=False)
    filled = db.Column(db.Boolean, nullable=False)
    raw_text = db.Column(db.String(200))
    analyst = db.Column(db.String(120), default='unknown')
    checked_at = db.Column(db.DateTime, default=datetime.utcnow)

def create_tables():
    db.create_all()

# --- Helpers ---
def find_or_create_field(name):
    name = name.strip()
    if not name:
        return None
    f = Field.query.filter_by(canonical=name).first()
    if f:
        return f
    f = Field(canonical=name)
    db.session.add(f)
    db.session.commit()
    return f

# --- Routes / API ---
@app.route('/')
def index():
    return render_template_string(INDEX_HTML)

@app.route('/api/fields')
def api_fields():
    q = request.args.get('q','').strip()
    if q:
        results = Field.query.filter(Field.canonical.ilike(f'%{q}%')).limit(20).all()
    else:
        results = Field.query.order_by(Field.created_at.desc()).limit(50).all()
    return jsonify([{'id':f.id,'canonical':f.canonical} for f in results])

@app.route('/api/batches/<batch>/listings', methods=['POST'])
def add_listing(batch):
    data = request.json or {}
    listing_text = data.get('listing_id','').strip()
    observations = data.get('observations',[])
    analyst = data.get('analyst','unknown')
    if not listing_text:
        return jsonify({'error':'listing_id required'}), 400
    lst = Listing(batch=batch, listing_id_text=listing_text)
    db.session.add(lst)
    db.session.commit()
    for obs in observations:
        raw = (obs.get('field_text') or '').strip()
        if not raw:
            continue
        fld = Field.query.filter_by(canonical=raw).first()
        if not fld:
            fld = Field(canonical=raw)
            db.session.add(fld); db.session.commit()
        ob = Observation(listing_id=lst.id, field_id=fld.id, filled=bool(obs.get('filled',False)),
                         raw_text=raw, analyst=analyst)
        db.session.add(ob)
    db.session.commit()
    return jsonify({'status':'ok','listing_db_id':lst.id})

@app.route('/api/batches/<batch>/summary')
def batch_summary(batch):
    sql = '''SELECT f.id, f.canonical,
             SUM(CASE WHEN o.filled=1 THEN 1 ELSE 0 END) as filled_count,
             SUM(CASE WHEN o.filled=0 THEN 1 ELSE 0 END) as empty_count,
             COUNT(o.id) as sample_count
             FROM field f JOIN observation o ON f.id=o.field_id
             JOIN listing l on o.listing_id=l.id
             WHERE l.batch = :batch
             GROUP BY f.id
             ORDER BY sample_count DESC
          '''
    res = db.session.execute(sql, {'batch':batch}).fetchall()
    out = []
    for r in res:
        out.append({'field_id':r[0],'canonical':r[1],'filled':int(r[2]),'empty':int(r[3]),'sample':int(r[4])})
    return jsonify(out)

@app.route('/field/<int:field_id>')
def field_detail(field_id):
    fld = Field.query.get_or_404(field_id)
    listings = Listing.query.order_by(Listing.created_at.desc()).all()
    rows = []
    for l in listings:
        obs = Observation.query.filter_by(listing_id=l.id, field_id=field_id).first()
        if obs:
            status = 'filled' if obs.filled else 'empty'
        else:
            status = 'unchecked'
        rows.append({'listing_id_text': l.listing_id_text, 'status': status, 'obs_id': obs.id if obs else None})
    return render_template_string(FIELD_HTML, field=fld, rows=rows)

@app.route('/field/<int:field_id>/bulk_mark_empty', methods=['POST'])
def bulk_mark_empty(field_id):
    analyst = request.form.get('analyst','bulk_user')
    fld = Field.query.get_or_404(field_id)
    listings = Listing.query.all()
    count = 0
    for l in listings:
        obs = Observation.query.filter_by(listing_id=l.id, field_id=field_id).first()
        if not obs:
            ob = Observation(listing_id=l.id, field_id=field_id, filled=False, raw_text=fld.canonical, analyst=analyst)
            db.session.add(ob)
            count += 1
    db.session.commit()
    return redirect(url_for('field_detail', field_id=field_id))

@app.route('/export/observations.csv')
def export_observations():
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['listing_id','field','filled','analyst','checked_at'])
    obs = Observation.query.join(Listing, Observation.listing_id==Listing.id).join(Field, Observation.field_id==Field.id).add_columns(Listing.listing_id_text, Field.canonical, Observation.filled, Observation.analyst, Observation.checked_at).all()
    for o in obs:
        cw.writerow([o[1], o[2], int(o[3]), o[4], o[5].isoformat()])
    output = io.BytesIO()
    output.write(si.getvalue().encode())
    output.seek(0)
    return send_file(output, mimetype='text/csv', download_name='observations.csv', as_attachment=True)

@app.route('/import/observations', methods=['GET','POST'])
def import_obs():
    if request.method=='GET':
        return render_template_string(IMPORT_HTML)
    f = request.files.get('file')
    batch = request.form.get('batch','default')
    analyst = request.form.get('analyst','import_user')
    if not f:
        return "no file", 400
    text = f.read().decode()
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        listing_text = row.get('listing_id','').strip()
        field_name = row.get('field','').strip()
        filled = bool(int(row.get('filled','0')))
        if not listing_text or not field_name:
            continue
        lst = Listing.query.filter_by(listing_id_text=listing_text, batch=batch).first()
        if not lst:
            lst = Listing(batch=batch, listing_id_text=listing_text)
            db.session.add(lst); db.session.commit()
        fld = Field.query.filter_by(canonical=field_name).first()
        if not fld:
            fld = Field(canonical=field_name)
            db.session.add(fld); db.session.commit()
        obs = Observation(listing_id=lst.id, field_id=fld.id, filled=filled, raw_text=field_name, analyst=analyst)
        db.session.add(obs)
    db.session.commit()
    return "imported", 200

# Google Sheets helper functions (optional)
GSPREAD_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

def get_gspread_client():
    if not GSPREAD_AVAILABLE:
        raise RuntimeError("gspread/pandas not installed or unable to import")
    b64 = os.environ.get('GSPREAD_SERVICE_ACCOUNT_JSON_B64')
    if not b64:
        raise RuntimeError("Missing GSPREAD_SERVICE_ACCOUNT_JSON_B64 env var with base64 service-account JSON")
    raw = base64.b64decode(b64)
    info = json.loads(raw)
    creds = Credentials.from_service_account_info(info, scopes=GSPREAD_SCOPES)
    client = gspread.authorize(creds)
    return client

def build_listing_order(batch='default'):
    listings = Listing.query.filter_by(batch=batch).order_by(Listing.created_at.asc()).all()
    return listings

@app.route('/export/google_sheet_symbols')
def export_google_sheet_symbols():
    if not GSPREAD_AVAILABLE:
        return "gspread/pandas not installed or unable to import", 500

    sheet_id = request.args.get('sheet_id')
    batch = request.args.get('batch','default')
    if not sheet_id:
        return "missing sheet_id", 400

    try:
        client = get_gspread_client()
        sh = client.open_by_key(sheet_id)
    except Exception as e:
        return f"gspread open error: {e}", 500

    listings = build_listing_order(batch=batch)
    if not listings:
        return f"No listings found for batch '{batch}'.", 400

    listing_headers = []
    for i, l in enumerate(listings, start=1):
        header = f"L{i} - {l.listing_id_text}"
        listing_headers.append(header)

    fields = Field.query.order_by(Field.canonical.asc()).all()
    if not fields:
        return "No fields found.", 400

    header_row = ['Field Name'] + listing_headers + ['Filled Count', 'Empty Count', '% Empty', 'Remove? (≥6)']

    rows = []
    # build obs map
    obs_q = Observation.query.all()
    obs_map = {(o.field_id, o.listing_id): o for o in obs_q}

    for field in fields:
        row = [field.canonical]
        for l in listings:
            key = (field.id, l.id)
            obs = obs_map.get(key)
            if obs is None:
                symbol = '—'
            else:
                symbol = '✔️' if obs.filled else '✖️'
            row.append(symbol)
        rows.append(row)

    try:
        try:
            ws = sh.worksheet('Single Family')
            sh.del_worksheet(ws)
        except Exception:
            pass
        num_rows = max(100, len(rows) + 10)
        num_cols = len(header_row)
        ws = sh.add_worksheet(title='Single Family', rows=str(num_rows), cols=str(num_cols))
    except Exception as e:
        return f"sheet creation error: {e}", 500

    try:
        ws.update('A1', [header_row])
    except Exception as e:
        return f"error writing header: {e}", 500

    try:
        ws.update('A2', rows)
    except Exception as e:
        return f"error writing rows: {e}", 500

    # summary formula columns
    n_listings = len(listings)
    first_listing_col = 2
    last_listing_col = 1 + n_listings
    filled_col_idx = last_listing_col + 1
    empty_col_idx = last_listing_col + 2
    pct_col_idx = last_listing_col + 3
    remove_col_idx = last_listing_col + 4

    def col_idx_to_letter(idx):
        letters = ''
        while idx > 0:
            idx, rem = divmod(idx-1, 26)
            letters = chr(65 + rem) + letters
        return letters

    filled_col_letter = col_idx_to_letter(filled_col_idx)
    empty_col_letter = col_idx_to_letter(empty_col_idx)
    pct_col_letter = col_idx_to_letter(pct_col_idx)
    remove_col_letter = col_idx_to_letter(remove_col_idx)
    first_list_col_letter = col_idx_to_letter(first_listing_col)
    last_list_col_letter = col_idx_to_letter(last_listing_col)

    for i in range(len(rows)):
        row_num = 2 + i
        list_range = f"{first_list_col_letter}{row_num}:{last_list_col_letter}{row_num}"
        filled_formula = f'=COUNTIF({list_range},"✔️")'
        empty_formula = f'=COUNTIF({list_range},"✖️")'
        pct_formula = f'=IF(({filled_col_letter}{row_num}+{empty_col_letter}{row_num})=0,0,{empty_col_letter}{row_num}/({filled_col_letter}{row_num}+{empty_col_letter}{row_num}))'
        remove_formula = f'=IF(AND({empty_col_letter}{row_num}>=6,({filled_col_letter}{row_num}+{empty_col_letter}{row_num})>=10),"YES","NO")'
        try:
            ws.update_acell(f'{filled_col_letter}{row_num}', filled_formula)
            ws.update_acell(f'{empty_col_letter}{row_num}', empty_formula)
            ws.update_acell(f'{pct_col_letter}{row_num}', pct_formula)
            ws.update_acell(f'{remove_col_letter}{row_num}', remove_formula)
        except Exception as e:
            return f"error writing formulas at row {row_num}: {e}", 500

    # optionally set percent format via gspread-formatting if needed (not included)
    return f"Exported {len(rows)} fields for {n_listings} listings to sheet {sheet_id} (tab 'Single Family')", 200

# --- HTML templates (inline strings) ---
INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>MLS Cleanup — Quick Entry</title>
  <style>
    body{font-family:system-ui,Segoe UI,Arial;margin:18px}
    .tag{display:inline-block;padding:6px;border-radius:6px;border:1px solid #bbb;margin:4px}
    .tag input[type=checkbox]{margin-left:8px}
    .field-list{margin-top:10px}
    #suggestions{border:1px solid #ddd;padding:6px;max-height:140px;overflow:auto}
    input[type=text]{padding:6px}
    button{padding:6px 10px;margin-left:6px}
  </style>
</head>
<body>
  <h2>Quick Entry — Add Listing Observations</h2>
  <div>
    <label>Batch: <input id="batch" value="default"></label>
    <label style="margin-left:12px">Analyst: <input id="analyst" value="you"></label>
  </div>
  <div style="margin-top:8px">
    <label>Listing ID: <input id="listing_id" /></label>
    <button id="start_add">Start</button>
  </div>

  <div id="entry_area" style="display:none;margin-top:10px">
    <div>
      <input id="field_input" placeholder="Type field name and press Enter or choose suggestion" style="width:420px"/>
      <button id="add_field_btn">Add Field</button>
    </div>
    <div id="suggestions"></div>
    <div class="field-list" id="tags"></div>
    <div style="margin-top:8px">
      <button id="save_listing">Save Listing</button>
      <button onclick="location.href='/summary'">Go to Summary</button>
    </div>
  </div>

<script>
let tags = []; // {name, filled}
const listingInput = document.getElementById('listing_id');
document.getElementById('start_add').onclick = ()=>{
  if(!listingInput.value.trim()){ alert('enter listing id'); return; }
  document.getElementById('entry_area').style.display='block';
}
const fieldInput = document.getElementById('field_input');
fieldInput.addEventListener('input', async (e)=>{
  const q = fieldInput.value;
  const res = await fetch('/api/fields?q='+encodeURIComponent(q));
  const data = await res.json();
  const s = document.getElementById('suggestions');
  s.innerHTML = data.map(d=>`<div><a href="#" onclick="pickSuggestion('${escapeHtml(d.canonical)}');return false">${escapeHtml(d.canonical)}</a></div>`).join('');
});
function escapeHtml(s){ return s.replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function pickSuggestion(name){
  fieldInput.value = name;
  addTagFromInput();
}
document.getElementById('add_field_btn').onclick = addTagFromInput;
fieldInput.addEventListener('keydown', (e)=> { if(e.key==='Enter'){ e.preventDefault(); addTagFromInput(); }});
function addTagFromInput(){
  const name = fieldInput.value.trim();
  if(!name) return;
  tags.push({name, filled:true});
  renderTags();
  fieldInput.value='';
  document.getElementById('suggestions').innerHTML='';
}
function renderTags(){
  const container = document.getElementById('tags');
  container.innerHTML = tags.map((t,i)=>`<span class="tag">${escapeHtml(t.name)} <label><input type="checkbox" onchange="toggle(${i})" ${(t.filled?'checked':'')}> filled</label> <a href="#" onclick="removeTag(${i});return false">✕</a></span>`).join('');
}
function toggle(i){ tags[i].filled = !tags[i].filled; renderTags(); }
function removeTag(i){ tags.splice(i,1); renderTags(); }

document.getElementById('save_listing').onclick = async ()=>{
  const batch = document.getElementById('batch').value || 'default';
  const analyst = document.getElementById('analyst').value || 'you';
  const listing_id = listingInput.value.trim();
  if(!listing_id){ alert('enter listing id'); return; }
  const observations = tags.map(t=>({field_text:t.name, filled: !!t.filled}));
  const res = await fetch('/api/batches/'+encodeURIComponent(batch)+'/listings', {
    method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({listing_id, observations, analyst})
  });
  const data = await res.json();
  if(data.status==='ok'){ alert('saved'); tags=[]; renderTags(); document.getElementById('entry_area').style.display='none'; listingInput.value='';}
  else alert('error: '+JSON.stringify(data));
}
</script>

</body>
</html>
"""

FIELD_HTML = """
<!doctype html>
<title>Field Detail - {{ field.canonical }}</title>
<h3>Field: {{ field.canonical }}</h3>
<p><a href="/">Back to quick entry</a> — <a href="/summary">Summary</a></p>
<form method="post" action="{{ url_for('bulk_mark_empty', field_id=field.id) }}">
  <label>Analyst name for bulk action: <input name="analyst" value="bulk_user"></label>
  <button type="submit">Mark ALL UNCHECKED as EMPTY (bulk)</button>
</form>
<table border=1 cellpadding=6 style="margin-top:12px">
  <tr><th>Listing ID</th><th>Status</th></tr>
  {% for r in rows %}
    <tr>
      <td>{{ r.listing_id_text }}</td>
      <td>{{ r.status }}</td>
    </tr>
  {% endfor %}
</table>
"""

IMPORT_HTML = """
<!doctype html>
<title>Import Observations</title>
<h3>Import CSV</h3>
<p>CSV columns: listing_id,field,filled (0/1)</p>
<form method="post" enctype="multipart/form-data">
  <label>Batch: <input name="batch" value="default"></label>
  <label>Analyst: <input name="analyst" value="import_user"></label>
  <input type="file" name="file">
  <button type="submit">Import</button>
</form>
"""

SUMMARY_HTML = """
<!doctype html>
<title>Summary</title>
<h3>Batch Summary</h3>
<p><a href="/">Back</a> — <a href="/import/observations">Import CSV</a> — <a href="/export/observations.csv">Export CSV</a></p>
<div>
  <label>Batch: <input id="batch" value="default"></label>
  <label style="margin-left:12px">Threshold empty_count >= <input id="abs_thresh" value="6" style="width:60px"></label>
  <label style="margin-left:12px">Min sample >= <input id="min_sample" value="10" style="width:60px"></label>
  <button onclick="load()">Load</button>
</div>
<canvas id="chart" width="800" height="300"></canvas>
<table border=1 cellpadding=6 id="tbl" style="margin-top:12px"></table>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
async function load(){
  const batch = document.getElementById('batch').value || 'default';
  const res = await fetch('/api/batches/'+encodeURIComponent(batch)+'/summary');
  const data = await res.json();
  renderTable(data);
  renderChart(data);
}
function renderTable(data){
  const abs = parseInt(document.getElementById('abs_thresh').value)||6;
  const minSample = parseInt(document.getElementById('min_sample').value)||10;
  const tbl = document.getElementById('tbl');
  tbl.innerHTML = '<tr><th>Field</th><th>sample</th><th>filled</th><th>empty</th><th>candidate?</th><th>action</th></tr>';
  data.forEach(d=>{
    const candidate = (d.empty >= abs && d.sample >= minSample) ? 'YES' : '';
    tbl.innerHTML += `<tr>
      <td>${d.canonical}</td>
      <td>${d.sample}</td>
      <td>${d.filled}</td>
      <td>${d.empty}</td>
      <td style="color:red">${candidate}</td>
      <td><a href="/field/${d.field_id}">review</a></td>
    </tr>`;
  });
}
function renderChart(data){
  const labels = data.map(d=>d.canonical);
  const filled = data.map(d=>d.filled);
  const empty = data.map(d=>d.empty);
  const ctx = document.getElementById('chart').getContext('2d');
  if(window._chart) window._chart.destroy();
  window._chart = new Chart(ctx, {
    type:'bar',
    data:{
      labels,
      datasets:[
        { label:'filled', data: filled, stack:'s' },
        { label:'empty', data: empty, stack:'s' }
      ]
    },
    options:{ responsive:true, interaction:{mode:'index', intersect:false} }
  });
}
load();
</script>
"""

if __name__ == '__main__':
    # ensure table exists before the server starts
    with app.app_context():
        create_tables()
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, port=port)
