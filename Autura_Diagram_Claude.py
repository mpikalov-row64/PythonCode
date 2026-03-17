import pandas as pd
import json
import math
from fontTools.ttLib import TTFont
from fontTools.pens.basePen import BasePen
from shapely.geometry import Polygon
from shapely.ops import unary_union

# Configuration
INPUT_FILE = r"C:\Users\mikha\Customers\Autura\lots.xlsx" 
SHEET_NAME = "Sheet0"
OUTPUT_GEOJSON_SHAPES = r"C:\Users\mikha\Customers\Autura\parking_shapes.geojson"
OUTPUT_GEOJSON_LABELS = r"C:\Users\mikha\Customers\Autura\parking_labels.geojson"

# Layout parameters
STALL_WIDTH = 40
STALL_HEIGHT = 80
GAP = 10
COLUMNS = 30
SECTION_GAP_Y = 100
LOT_GAP_X = 3000
TOP_PADDING = 100
LEFT_PADDING = 100
FONT_PATH = r"C:\Windows\Fonts\ARIALN.TTF"

# Font sizes
STALL_LABEL_HEIGHT = 12
SECTION_LABEL_HEIGHT = 18
LOT_LABEL_HEIGHT = 24
STROKE_WIDTH = 2.5


# ============================================================================
# TEXT RENDERING (from version 22 - LineStrings)
# ============================================================================
class GlyphToOutlinePen(BasePen):
    """Converts glyph outlines to line strings (no fill, only strokes)"""
    def __init__(self, glyphSet):
        BasePen.__init__(self, glyphSet)
        self.paths = []
        self.currentPath = []

    def _moveTo(self, p0):
        if self.currentPath:
            self.paths.append(self.currentPath)
        self.currentPath = [p0]

    def _lineTo(self, p1):
        self.currentPath.append(p1)

    def _qCurveToOne(self, p1, p2):
        if self.currentPath:
            p0 = self.currentPath[-1]
            for t in [0.2, 0.4, 0.6, 0.8, 1.0]:
                x = (1-t)**2 * p0[0] + 2*(1-t)*t * p1[0] + t**2 * p2[0]
                y = (1-t)**2 * p0[1] + 2*(1-t)*t * p1[1] + t**2 * p2[1]
                self.currentPath.append((x, y))

    def _curveToOne(self, p1, p2, p3):
        if self.currentPath:
            p0 = self.currentPath[-1]
            for t in [0.15, 0.3, 0.45, 0.6, 0.75, 0.9, 1.0]:
                x = (1-t)**3 * p0[0] + 3*(1-t)**2*t * p1[0] + 3*(1-t)*t**2 * p2[0] + t**3 * p3[0]
                y = (1-t)**3 * p0[1] + 3*(1-t)**2*t * p1[1] + 3*(1-t)*t**2 * p2[1] + t**3 * p3[1]
                self.currentPath.append((x, y))

    def _closePath(self):
        if self.currentPath and len(self.currentPath) > 1:
            self.currentPath.append(self.currentPath[0])
            self.paths.append(self.currentPath)
            self.currentPath = []

    def getPaths(self):
        if self.currentPath and len(self.currentPath) > 1:
            self.currentPath.append(self.currentPath[0])
            self.paths.append(self.currentPath)
            self.currentPath = []
        return self.paths


def text_to_linestring_features(text, x, y, target_height, font, glyph_set, cmap, em, label_type="stall", extra_props=None):
    """Convert text to GeoJSON LineString features (outlines only, no fill)"""
    features = []
    
    glyphs = []
    for char in text:
        glyph_index = cmap.get(ord(char))
        if glyph_index:
            g = glyph_set[glyph_index]
            glyphs.append(g)
    
    if not glyphs:
        return features
    
    total_width_units = sum([g.width for g in glyphs])
    if total_width_units == 0:
        total_width_units = 1
    
    scale = target_height / em
    total_width = total_width_units * scale
    
    start_x = x - total_width / 2
    start_y = y - target_height / 2
    
    cursor_x = start_x
    for glyph in glyphs:
        pen = GlyphToOutlinePen(glyph_set)
        glyph.draw(pen)
        paths = pen.getPaths()
        
        for path in paths:
            if len(path) < 2:
                continue
            
            line_coords = [[cursor_x + px*scale, start_y + py*scale] 
                          for px, py in path]
            
            props = {
                "label": text, 
                "type": label_type,
                "stroke_width": STROKE_WIDTH
            }
            if extra_props:
                props.update(extra_props)
            
            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": line_coords},
                "properties": props
            })
        
        cursor_x += glyph.width * scale
    
    return features


# ============================================================================
# MAIN LAYOUT (boxes from version 2, text from version 22)
# ============================================================================

# Load font
font = TTFont(FONT_PATH)
glyph_set = font.getGlyphSet()
cmap = font.getBestCmap()
em = font['head'].unitsPerEm

# Load data
df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, usecols=[0,1,2,3])
df.columns = ["LotCode","Lotid","LotSection","LotStall"]
df["LotStall"] = df["LotStall"].astype(str).str.replace("Space", "").str.strip()
df = df.drop_duplicates(subset=["LotCode","LotSection","LotStall"]).reset_index(drop=True)
df = df.sort_values(["LotCode","LotSection","LotStall"]).reset_index(drop=True)

shape_features = []
label_features = []

lot_codes = df["LotCode"].unique()

# VERSION 2 BOX LAYOUT
for lot_idx, lot_code in enumerate(lot_codes):
    lot_df = df[df["LotCode"] == lot_code]
    lot_x = LEFT_PADDING + lot_idx * LOT_GAP_X
    current_y = TOP_PADDING
    
    # LOT LABEL (aligned to left edge where boxes start)
    lot_label_x = lot_x
    lot_label_y = current_y - 60
    lot_text = f"LOT {lot_code}"
    label_features.extend(
        text_to_linestring_features(lot_text, lot_label_x, lot_label_y, LOT_LABEL_HEIGHT, 
                                    font, glyph_set, cmap, em, "lot_label", 
                                    {"LotCode": lot_code})
    )

    for section in lot_df["LotSection"].unique():
        section_df = lot_df[lot_df["LotSection"] == section].reset_index(drop=True)
        num_stalls = len(section_df)
        rows_needed = math.ceil(num_stalls / COLUMNS)
        
        # SECTION LABEL (exact position from version 2)
        section_label_x = lot_x - 40
        section_label_y = current_y + (rows_needed * (STALL_HEIGHT + GAP)) / 2
        section_text = f"SEC {section}"
        label_features.extend(
            text_to_linestring_features(section_text, section_label_x, section_label_y, 
                                       SECTION_LABEL_HEIGHT, font, glyph_set, cmap, em, 
                                       "section_label", {"LotSection": section})
        )
        
        # Section separator line (adjusted to match version 2 calculations)
        separator_height = rows_needed * (STALL_HEIGHT + GAP)
        shape_features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [lot_x - 15, current_y - 10],
                    [lot_x - 15, current_y + separator_height + 10]
                ]
            },
            "properties": {"type": "section_separator"}
        })

        # STALLS - VERSION 2 BOX FORMAT
        for i, row in section_df.iterrows():
            col = i % COLUMNS
            r = i // COLUMNS
            x = lot_x + col * (STALL_WIDTH + GAP)
            y = current_y + r * (STALL_HEIGHT + GAP)

            stall_id = row.LotStall
            geo_id = f"{row.LotCode}_{row.LotSection}_{stall_id}"

            # Stall rectangle (EXACT VERSION 2 format)
            poly_coords = [
                [x, y],
                [x + STALL_WIDTH, y],
                [x + STALL_WIDTH, y + STALL_HEIGHT],
                [x, y + STALL_HEIGHT],
                [x, y]
            ]
            shape_features.append({
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [poly_coords]},
                "properties": {
                    "GEOID": geo_id,
                    "LotCode": row.LotCode,
                    "LotSection": row.LotSection,
                    "StallNumber": stall_id
                }
            })

            # Stall label (VERSION 22 text rendering)
            cx = x + STALL_WIDTH / 2
            cy = y + STALL_HEIGHT / 2
            label_features.extend(
                text_to_linestring_features(stall_id, cx, cy, STALL_LABEL_HEIGHT, 
                                           font, glyph_set, cmap, em, "stall_label", None)
            )

        current_y += rows_needed * (STALL_HEIGHT + GAP) + SECTION_GAP_Y

# Save
geojson_shapes = {"type": "FeatureCollection", "features": shape_features}
geojson_labels = {"type": "FeatureCollection", "features": label_features}

with open(OUTPUT_GEOJSON_SHAPES, "w") as f:
    json.dump(geojson_shapes, f, indent=2)

with open(OUTPUT_GEOJSON_LABELS, "w") as f:
    json.dump(geojson_labels, f, indent=2)

print(f"✓ Generated {len(shape_features)} shapes (Polygons + separators)")
print(f"✓ Generated {len(label_features)} label outlines (LineStrings)")
print(f"✓ Shapes: {OUTPUT_GEOJSON_SHAPES}")
print(f"✓ Labels: {OUTPUT_GEOJSON_LABELS}")