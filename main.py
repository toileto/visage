import sqlglot
from sqlglot import exp
from collections import defaultdict
import json
import webbrowser
import os
import argparse
import glob


class Lineage:
    def __init__(self):
        self.tables = defaultdict(set)
        self.flow_edges = []
        self.join_edges = []
        self.defined_tables = set()
        self.ctes = set()

    def _get_full_table_name(self, table_exp):
        if not table_exp: return "Unknown"
        parts = [table_exp.catalog, table_exp.db, table_exp.name]
        full_name = ".".join([p for p in parts if p])
        return full_name.lower() if full_name else "unknown_table"

    def _get_col_name(self, expression):
        if expression.alias: return expression.alias
        if isinstance(expression, exp.Column): return expression.name
        return "calc_field"

    def _clean_id(self, name):
        if not name: return "Unknown"
        return name.replace(".", "_").replace(" ", "_")

    def _analyze_select(self, select_expression, target_table_full_name):
        if not target_table_full_name: target_table_full_name = "Unknown_Target"
        alias_map = {}

        # 1. Map Aliases
        for table in select_expression.find_all(exp.Table):
            full_name = self._get_full_table_name(table)
            alias = table.alias if table.alias else table.name
            alias_map[alias] = full_name
            if full_name not in self.tables: self.tables[full_name] = set()

        # 2. Map Data Flow
        for expression in select_expression.expressions:
            target_col = self._get_col_name(expression)
            if target_table_full_name not in self.tables: self.tables[
                target_table_full_name] = set()
            self.tables[target_table_full_name].add(target_col)

            for ref in expression.find_all(exp.Column):
                src_tbl_alias = ref.table
                src_col = ref.name
                if not src_tbl_alias and alias_map: src_tbl_alias = \
                    list(alias_map.keys())[0]
                real_source_full = alias_map.get(src_tbl_alias, src_tbl_alias)

                if real_source_full:
                    if real_source_full not in self.tables: self.tables[
                        real_source_full] = set()
                    self.tables[real_source_full].add(src_col)
                    self.flow_edges.append(
                        (real_source_full, src_col, target_table_full_name,
                         target_col))

        # 3. Map Joins
        for join in select_expression.find_all(exp.Join):
            on_clause = join.args.get("on")
            if on_clause:
                for eq in on_clause.find_all(exp.EQ):
                    left = eq.left
                    right = eq.right

                    def extract_full_ref(node):
                        if isinstance(node, exp.Column):
                            t_alias = node.table
                            c_name = node.name
                            if not t_alias and alias_map: t_alias = \
                                list(alias_map.keys())[0]
                            full_name = alias_map.get(t_alias, t_alias)
                            return full_name, c_name
                        return None, None

                    l_tbl, l_col = extract_full_ref(left)
                    r_tbl, r_col = extract_full_ref(right)

                    if l_tbl and r_tbl:
                        if l_tbl not in self.tables: self.tables[l_tbl] = set()
                        self.tables[l_tbl].add(l_col)
                        if r_tbl not in self.tables: self.tables[r_tbl] = set()
                        self.tables[r_tbl].add(r_col)
                        self.join_edges.append((l_tbl, l_col, r_tbl, r_col))

    def parse_sql(self, sql_code, output_table_name=None):
        if output_table_name:
            output_table_name = output_table_name.lower()
            self.defined_tables.add(output_table_name)
            # Ensure table exists in graph even if no lineage found
            self.tables[output_table_name]

        try:
            parsed = sqlglot.parse_one(sql_code)
            if parsed.find(exp.With):
                for cte in parsed.find_all(exp.CTE):
                    cte_name = cte.alias or "CTE"
                    self.ctes.add(cte_name)
                    self._analyze_select(cte.this, cte_name)

            target_full_name = output_table_name if output_table_name else "FINAL_OUTPUT"
            select_part = None

            if isinstance(parsed, exp.Insert):
                if not output_table_name:
                    target_full_name = self._get_full_table_name(parsed.this)
                select_part = parsed.expression
            elif isinstance(parsed, exp.Create):
                if not output_table_name:
                    target_full_name = self._get_full_table_name(parsed.this)
                select_part = parsed.expression
            elif isinstance(parsed, exp.Select):
                # target_full_name is already set
                select_part = parsed

            if select_part:
                self._analyze_select(select_part, target_full_name)
        except Exception as e:
            print(f"Error parsing SQL: {e}")

    def generate_interactive_html(self, filename="lineage_final.html"):
        elements = []
        for table in self.tables.keys():
            subtype = "cte" if table in self.ctes else "physical"
            elements.append({"data": {"id": self._clean_id(table),
                                      "label": table, "type": "table",
                                      "subtype": subtype},
                             "classes": "table_node"})
            for col in self.tables[table]:
                col_id = f"{self._clean_id(table)}_{self._clean_id(col)}"
                elements.append({"data": {"id": col_id, "label": col,
                                          "parent": self._clean_id(table),
                                          "type": "column"},
                                 "classes": "column_node"})

        for i, (src_tbl, src_col, tgt_tbl, tgt_col) in enumerate(
                self.flow_edges):
            src_id = f"{self._clean_id(src_tbl)}_{self._clean_id(src_col)}"
            tgt_id = f"{self._clean_id(tgt_tbl)}_{self._clean_id(tgt_col)}"
            elements.append({"data": {"id": f"flow_{i}", "source": src_id,
                                      "target": tgt_id, "edgeType": "flow"}})

        for i, (tbl1, col1, tbl2, col2) in enumerate(self.join_edges):
            id1 = f"{self._clean_id(tbl1)}_{self._clean_id(col1)}"
            id2 = f"{self._clean_id(tbl2)}_{self._clean_id(col2)}"
            elements.append({"data": {"id": f"join_{i}", "source": id1,
                                      "target": id2, "edgeType": "join"}})

        json_elements = json.dumps(elements)
        defined_tables_list = sorted(list(self.defined_tables))
        json_tables = json.dumps(defined_tables_list)

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Lineage Explorer</title>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.26.0/cytoscape.min.js"></script>
            <script src="https://unpkg.com/dagre@0.8.5/dist/dagre.js"></script>
            <script src="https://cytoscape.org/cytoscape.js-dagre/cytoscape-dagre.js"></script>
            <style>
                body {{ font-family: 'Segoe UI', sans-serif; margin: 0; background-color: #f8f9fa; }}

                /* Homepage Styles */
                #homepage {{ padding: 40px; max-width: 800px; margin: 0 auto; }}
                h1 {{ color: #2c3e50; text-align: center; }}
                .search-container {{ margin-bottom: 30px; text-align: center; }}
                #homeSearchInput {{ width: 100%; max-width: 500px; padding: 12px; font-size: 16px; border: 1px solid #ddd; border-radius: 25px; outline: none; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }}
                #tableList {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 15px; }}
                .table-card {{ background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); cursor: pointer; transition: transform 0.2s, box-shadow 0.2s; text-align: center; font-weight: bold; color: #34495e; }}
                .table-card:hover {{ transform: translateY(-3px); box-shadow: 0 5px 15px rgba(0,0,0,0.1); border-bottom: 3px solid #3498db; }}

                /* Graph View Styles */
                #graphView {{ display: none; width: 100vw; height: 100vh; position: relative; }}
                #cy {{ width: 100%; height: 100%; display: block; }}
                .controls {{
                    position: absolute; top: 20px; right: 20px; z-index: 999;
                    background: white; padding: 20px; border-radius: 8px;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.1); width: 280px;
                }}
                .back-btn {{ position: absolute; top: 20px; left: 20px; z-index: 999; background: #34495e; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-weight: bold; }}
                .back-btn:hover {{ background: #2c3e50; }}

                input.graph-search {{ width: 100%; padding: 8px; margin-bottom: 5px; box-sizing: border-box; }}
                .hint {{ font-size: 11px; color: #888; margin-bottom: 10px; font-style: italic; }}
                button.action-btn {{ width: 48%; padding: 8px; cursor: pointer; }}
                .legend {{ margin-top: 15px; font-size: 12px; color: #666; }}
                .legend-item {{ display: flex; align-items: center; margin-bottom: 5px; }}
                .dot {{ width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }}
                .legend-box {{ width: 12px; height: 12px; margin-right: 8px; border: 1px solid #ccc; }}
            </style>
        </head>
        <body>
            <!-- Homepage -->
            <div id="homepage">
                <h1>Data Lineage Explorer</h1>
                <div class="search-container">
                    <input type="text" id="homeSearchInput" placeholder="Search for a table..." onkeyup="filterTables()">
                </div>
                <div id="tableList"></div>
            </div>

            <!-- Graph View -->
            <div id="graphView">
                <button class="back-btn" onclick="showHome()">← Back to List</button>
                <div class="controls">
                    <h3 style="margin-top:0;" id="graphTitle">Lineage</h3>
                    <input type="text" id="graphSearchInput" class="graph-search" placeholder="Search columns...">
                    <div class="hint">Click a column OR search above</div>
                    <div>
                        <button class="action-btn" onclick="performGraphSearch()" style="background:#007bff; color:white; border:none;">Find</button>
                        <button class="action-btn" onclick="resetGraphView()" style="background:#6c757d; color:white; border:none;">Reset</button>
                    </div>
                    <div class="legend">
                        <div class="legend-item"><div class="dot" style="background:#95a5a6;"></div>Data Flow</div>
                        <div class="legend-item"><div class="dot" style="background:#e67e22;"></div>Join Key</div>
                        <hr style="margin: 8px 0; border: 0; border-top: 1px solid #eee;">
                        <div class="legend-item"><div class="legend-box" style="background:#3498db;"></div>Selected Table</div>
                        <div class="legend-item"><div class="legend-box" style="background:#95a5a6;"></div>Upstream</div>
                        <div class="legend-item"><div class="legend-box" style="background:#2ecc71;"></div>Downstream</div>
                        <div class="legend-item"><div class="legend-box" style="background:#fff; border: 2px dashed #2c3e50;"></div>CTE</div>
                    </div>
                </div>
                <div id="cy"></div>
            </div>

            <script>
                var elements = {json_elements};
                var definedTables = {json_tables};
                var cy = null;
                var currentRoot = null;

                // --- HOMEPAGE LOGIC ---
                function renderTableList(filterText = "") {{
                    var container = document.getElementById('tableList');
                    container.innerHTML = "";
                    var filtered = definedTables.filter(t => t.toLowerCase().includes(filterText.toLowerCase()));

                    filtered.forEach(t => {{
                        var card = document.createElement('div');
                        card.className = 'table-card';
                        card.innerText = t;
                        card.onclick = () => showLineage(t);
                        container.appendChild(card);
                    }});
                }}

                function filterTables() {{
                    var input = document.getElementById('homeSearchInput');
                    renderTableList(input.value);
                }}

                function showHome() {{
                    currentRoot = null;
                    document.getElementById('homepage').style.display = 'block';
                    document.getElementById('graphView').style.display = 'none';
                }}

                // --- GRAPH LOGIC ---
                function initCy() {{
                    if (cy) return;
                    cy = cytoscape({{
                        container: document.getElementById('cy'),
                        elements: elements,
                        style: [
                            {{ selector: '.table_node', style: {{ 'shape': 'roundrectangle', 'background-color': '#fff', 'border-width': 2, 'border-color': '#2c3e50', 'content': 'data(label)', 'text-valign': 'top', 'text-halign': 'center', 'font-weight': 'bold', 'padding': 12, 'font-size': 14, 'color': '#2c3e50' }} }},
                            {{ selector: 'node[subtype="cte"]', style: {{ 'border-style': 'dashed' }} }},
                            {{ selector: '.column_node', style: {{ 'shape': 'roundrectangle', 'width': 'label', 'height': 'label', 'padding': 8, 'background-color': '#ecf0f1', 'border-width': 1, 'border-color': '#bdc3c7', 'content': 'data(label)', 'text-valign': 'center', 'text-halign': 'center', 'font-size': 12, 'color': '#34495e' }} }},
                            {{ selector: 'edge[edgeType="flow"]', style: {{ 'width': 2, 'line-color': '#95a5a6', 'target-arrow-color': '#95a5a6', 'target-arrow-shape': 'triangle', 'curve-style': 'bezier' }} }},
                            {{ selector: 'edge[edgeType="join"]', style: {{ 'width': 2, 'line-color': '#e67e22', 'line-style': 'dashed', 'target-arrow-shape': 'none', 'curve-style': 'bezier' }} }},
                            {{ selector: '.highlighted', style: {{ 'background-color': '#3498db', 'line-color': '#2980b9', 'target-arrow-color': '#2980b9', 'border-color': '#2980b9', 'color': '#fff' }} }},
                            {{ selector: '.hidden', style: {{ 'display': 'none' }} }},

                            // Lineage Coloring
                            {{ selector: '.root-node', style: {{ 'background-color': '#eaf2f8', 'border-color': '#3498db', 'border-width': 4 }} }},
                            {{ selector: '.upstream-node', style: {{ 'background-color': '#f2f4f4', 'border-color': '#95a5a6' }} }},
                            {{ selector: '.downstream-node', style: {{ 'background-color': '#eafaf1', 'border-color': '#2ecc71' }} }}
                        ],
                        layout: {{ name: 'dagre', rankDir: 'LR', nodeSep: 50, rankSep: 150 }}
                    }});

                    // Click listener
                    cy.on('tap', 'node', function(evt){{
                        var node = evt.target;
                        if(node.data('type') === 'column') {{
                            focusOnNodes(cy.collection().union(node));
                            document.getElementById('graphSearchInput').value = node.data('label');
                        }}
                    }});
                }}

                function showLineage(tableName) {{
                    currentRoot = tableName;
                    document.getElementById('homepage').style.display = 'none';
                    document.getElementById('graphView').style.display = 'block';
                    document.getElementById('graphTitle').innerText = "Lineage: " + tableName;

                    initCy();

                    // Reset view first
                    cy.elements().removeClass('highlighted hidden root-node upstream-node downstream-node');

                    // Find the table node
                    var cleanName = tableName.replace(/\./g, "_").replace(/ /g, "_");
                    var targetNode = cy.getElementById(cleanName);

                    if (targetNode.length > 0) {{
                        // Get columns of the target table
                        var targetColumns = targetNode.children();

                        // --- UPSTREAM TRAVERSAL ---
                        var allUpstream = cy.collection();
                        var currentUp = targetColumns;
                        while(currentUp.length > 0) {{
                            allUpstream = allUpstream.union(currentUp);
                            var preds = currentUp.predecessors(); 
                            if (preds.length === 0) break;
                            allUpstream = allUpstream.union(preds);
                            var sources = preds.nodes();
                            currentUp = sources.difference(allUpstream);
                        }}
                        var upstreamTables = allUpstream.nodes().parents();

                        // --- DOWNSTREAM TRAVERSAL ---
                        var allDownstream = cy.collection();
                        var currentDown = targetColumns;
                        while(currentDown.length > 0) {{
                            allDownstream = allDownstream.union(currentDown);
                            var succs = currentDown.successors();
                            if (succs.length === 0) break;
                            allDownstream = allDownstream.union(succs);
                            var targets = succs.nodes();
                            currentDown = targets.difference(allDownstream);
                        }}
                        var downstreamTables = allDownstream.nodes().parents();

                        // --- COMBINE & SHOW ---
                        var finalSet = targetNode
                                        .union(allUpstream)
                                        .union(upstreamTables)
                                        .union(allDownstream)
                                        .union(downstreamTables);

                        cy.elements().not(finalSet).addClass('hidden');
                        finalSet.removeClass('hidden');

                        // --- APPLY STYLES ---
                        targetNode.addClass('root-node');
                        upstreamTables.difference(targetNode).addClass('upstream-node');
                        downstreamTables.difference(targetNode).addClass('downstream-node');

                        finalSet.layout({{ name: 'dagre', rankDir: 'LR', nodeSep: 30, rankSep: 100, fit: true, padding: 50 }}).run();
                    }} else {{
                        console.warn("Table node not found: " + cleanName);
                        cy.layout({{ name: 'dagre', rankDir: 'LR', nodeSep: 50, rankSep: 150 }}).run();
                        cy.fit();
                    }}
                }}

                function resetGraphView() {{
                    document.getElementById('graphSearchInput').value = '';
                    if (currentRoot) {{
                        showLineage(currentRoot);
                    }} else {{
                        cy.elements().removeClass('highlighted hidden root-node upstream-node downstream-node');
                        cy.layout({{ name: 'dagre', rankDir: 'LR', nodeSep: 50, rankSep: 150 }}).run();
                        cy.fit();
                    }}
                }}

                function focusOnNodes(collection) {{
                    if (collection.length === 0) return;
                    cy.elements().removeClass('highlighted hidden');
                    var journey = cy.collection();
                    collection.forEach(function(node){{
                        journey = journey.union(node);
                        journey = journey.union(node.successors());
                        journey = journey.union(node.predecessors());
                        journey = journey.union(node.neighborhood());
                    }});
                    var visibleSet = journey.union(journey.parents());
                    journey.addClass('highlighted');
                    cy.elements().not(visibleSet).addClass('hidden');
                    visibleSet.layout({{ name: 'dagre', rankDir: 'LR', nodeSep: 30, rankSep: 100, fit: true, padding: 50 }}).run();
                }}

                function performGraphSearch() {{
                    var rawInput = document.getElementById('graphSearchInput').value;
                    if (!rawInput || !rawInput.trim()) {{ resetGraphView(); return; }}
                    var terms = rawInput.split(/[;,]+/).map(t => t.trim().toLowerCase()).filter(t => t.length > 0);
                    var matches = cy.nodes().filter(function(ele){{
                        if (ele.data('type') !== 'column') return false;
                        if (ele.hasClass('hidden')) return false; // Only search visible
                        var label = ele.data('label').toLowerCase();
                        return terms.some(function(term) {{ return label.includes(term); }});
                    }});
                    if (matches.length > 0) focusOnNodes(matches);
                }}

                // Init homepage
                renderTableList();
            </script>
        </body>
        </html>
        """

        _filename = filename.replace('.sql', '.html')
        if not _filename.endswith('.html'): _filename += '.html'

        with open(_filename, "w") as f:
            f.write(html_content)
        print(f"File created: {_filename}")


if __name__ == "__main__":
    # Load config
    config_path = "config.json"
    repo_path = "."
    output_file = "index.html"

    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = json.load(f)
            repo_path = config.get("sql_repo_path", ".")
            output_file = config.get("output_file", "index.html")

    tool = Lineage()

    # Scan SQL files
    search_pattern = os.path.join(repo_path, "*.sql")
    files = glob.glob(search_pattern)

    print(f"Found {len(files)} SQL files in {repo_path}")

    for file_path in files:
        filename = os.path.basename(file_path)
        table_name = os.path.splitext(filename)[0]

        with open(file_path, "r") as f:
            sql_content = f.read()

        print(f"Parsing {filename} as table {table_name}...")
        tool.parse_sql(sql_content, output_table_name=table_name)

    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    tool.generate_interactive_html(filename=output_file)
