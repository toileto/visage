import sqlglot
from sqlglot import exp
from collections import defaultdict
import json
import webbrowser
import os
import argparse # Import argparse


class Lineage:
    def __init__(self):
        self.tables = defaultdict(set)
        self.flow_edges = []
        self.join_edges = []

    def _get_full_table_name(self, table_exp):
        if not table_exp: return "Unknown"
        parts = [table_exp.catalog, table_exp.db, table_exp.name]
        full_name = ".".join([p for p in parts if p])
        return full_name if full_name else "Unknown_Table"

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

    def parse_sql(self, sql_code):
        try:
            parsed = sqlglot.parse_one(sql_code)
            if parsed.find(exp.With):
                for cte in parsed.find_all(exp.CTE):
                    cte_name = cte.alias or "CTE"
                    self._analyze_select(cte.this, cte_name)

            target_full_name = "FINAL_OUTPUT"
            select_part = None

            if isinstance(parsed, exp.Insert):
                target_full_name = self._get_full_table_name(parsed.this)
                select_part = parsed.expression
            elif isinstance(parsed, exp.Create):
                target_full_name = self._get_full_table_name(parsed.this)
                select_part = parsed.expression
            elif isinstance(parsed, exp.Select):
                target_full_name = "RESULT_GRID"
                select_part = parsed

            if select_part:
                self._analyze_select(select_part, target_full_name)
        except Exception as e:
            print(f"Error parsing SQL: {e}")

    def generate_interactive_html(self, filename="lineage_final.html"):
        elements = []
        for table in self.tables.keys():
            elements.append({"data": {"id": self._clean_id(table),
                                      "label": table, "type": "table"},
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

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{filename}</title>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.26.0/cytoscape.min.js"></script>
            <script src="https://unpkg.com/dagre@0.8.5/dist/dagre.js"></script>
            <script src="https://cytoscape.org/cytoscape.js-dagre/cytoscape-dagre.js"></script>
            <style>
                body {{ font-family: 'Segoe UI', sans-serif; margin: 0; background-color: #f8f9fa; }}
                #cy {{ width: 100vw; height: 100vh; display: block; }}
                .controls {{
                    position: absolute; top: 20px; right: 20px; z-index: 999;
                    background: white; padding: 20px; border-radius: 8px;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.1); width: 280px;
                }}
                input {{ width: 100%; padding: 8px; margin-bottom: 5px; box-sizing: border-box; }}
                .hint {{ font-size: 11px; color: #888; margin-bottom: 10px; font-style: italic; }}
                button {{ width: 48%; padding: 8px; cursor: pointer; }}
                .legend {{ margin-top: 15px; font-size: 12px; color: #666; }}
                .legend-item {{ display: flex; align-items: center; margin-bottom: 5px; }}
                .dot {{ width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }}
            </style>
        </head>
        <body>
            <div class="controls">
                <h3 style="margin-top:0;">Data Lineage</h3>
                <input type="text" id="searchInput" placeholder="Search (e.g. id, price; email)">
                <div class="hint">Click a column OR search above</div>
                <div>
                    <button onclick="performSearch()" style="background:#007bff; color:white; border:none;">Find</button>
                    <button onclick="resetView()" style="background:#6c757d; color:white; border:none;">Reset</button>
                </div>
                <div class="legend">
                    <div class="legend-item"><div class="dot" style="background:#95a5a6;"></div>Data Flow</div>
                    <div class="legend-item"><div class="dot" style="background:#e67e22;"></div>Join Key</div>
                </div>
            </div>
            <div id="cy"></div>
            <script>
                var elements = {json_elements};
                var cy = cytoscape({{
                    container: document.getElementById('cy'),
                    elements: elements,
                    style: [
                        {{ selector: '.table_node', style: {{ 'shape': 'roundrectangle', 'background-color': '#fff', 'border-width': 2, 'border-color': '#2c3e50', 'content': 'data(label)', 'text-valign': 'top', 'text-halign': 'center', 'font-weight': 'bold', 'padding': 12, 'font-size': 14, 'color': '#2c3e50' }} }},
                        {{ selector: '.column_node', style: {{ 'shape': 'roundrectangle', 'width': 'label', 'height': 'label', 'padding': 8, 'background-color': '#ecf0f1', 'border-width': 1, 'border-color': '#bdc3c7', 'content': 'data(label)', 'text-valign': 'center', 'text-halign': 'center', 'font-size': 12, 'color': '#34495e' }} }},
                        {{ selector: 'edge[edgeType="flow"]', style: {{ 'width': 2, 'line-color': '#95a5a6', 'target-arrow-color': '#95a5a6', 'target-arrow-shape': 'triangle', 'curve-style': 'bezier' }} }},
                        {{ selector: 'edge[edgeType="join"]', style: {{ 'width': 2, 'line-color': '#e67e22', 'line-style': 'dashed', 'target-arrow-shape': 'none', 'curve-style': 'bezier' }} }},
                        {{ selector: '.highlighted', style: {{ 'background-color': '#3498db', 'line-color': '#2980b9', 'target-arrow-color': '#2980b9', 'border-color': '#2980b9', 'color': '#fff' }} }},
                        {{ selector: '.hidden', style: {{ 'display': 'none' }} }}
                    ],
                    layout: {{ name: 'dagre', rankDir: 'LR', nodeSep: 50, rankSep: 150 }}
                }});

                function resetView() {{
                    cy.elements().removeClass('highlighted hidden');
                    document.getElementById('searchInput').value = '';
                    cy.layout({{ name: 'dagre', rankDir: 'LR', nodeSep: 50, rankSep: 150 }}).run();
                    cy.fit();
                }}

                // --- SHARED FOCUS LOGIC ---
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

                // --- SEARCH HANDLER ---
                function performSearch() {{
                    var rawInput = document.getElementById('searchInput').value;
                    if (!rawInput || !rawInput.trim()) {{ resetView(); return; }}

                    var terms = rawInput.split(/[;,]+/).map(t => t.trim().toLowerCase()).filter(t => t.length > 0);
                    if (terms.length === 0) {{ resetView(); return; }}

                    var matches = cy.nodes().filter(function(ele){{
                        if (ele.data('type') !== 'column') return false;
                        var label = ele.data('label').toLowerCase();
                        return terms.some(function(term) {{ return label.includes(term); }});
                    }});

                    if (matches.length > 0) focusOnNodes(matches);
                }}

                // --- CLICK LISTENER (Restored!) ---
                cy.on('tap', 'node', function(evt){{
                    var node = evt.target;
                    // Only react if clicking a column, not the table container
                    if(node.data('type') === 'column') {{
                        focusOnNodes(cy.collection().union(node));
                        // Update search box to show what was clicked
                        document.getElementById('searchInput').value = node.data('label');
                    }}
                }});

                // Allow "Enter" key for search
                document.getElementById('searchInput').addEventListener("keyup", function(event) {{
                    if (event.key === "Enter") performSearch();
                }});
            </script>
        </body>
        </html>
        """

        _filename = filename.replace('sql', 'html')

        with open(_filename, "w") as f:
            f.write(html_content)
            f.close()
        print(f"File created: {_filename}")
        # webbrowser.open('file://' + os.path.realpath(_filename))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL html visualization.")
    parser.add_argument("filename", nargs="?", default="query.sql",
                        help="The SQL file to analyze (default: query.sql)")
    args = parser.parse_args()

    with open(args.filename, "r") as f:
        complex_sql = f.read()
        f.close()

    tool = Lineage()
    tool.parse_sql(complex_sql)
    tool.generate_interactive_html(filename=args.filename)
