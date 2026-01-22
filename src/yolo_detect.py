import os
import csv
from ultralytics import YOLO

IMAGE_DIR = "data/raw/images"
OUTPUT_CSV = "data/raw/yolo_detections.csv"

model = YOLO("yolov8n.pt")

rows = []

for root, _, files in os.walk(IMAGE_DIR):
    channel_name = os.path.basename(root)

    for file in files:
        if file.lower().endswith((".jpg", ".jpeg", ".png")):
            image_path = os.path.join(root, file)
            message_id = os.path.splitext(file)[0]

            results = model(image_path, verbose=False)

            if not results:
                continue

            for r in results:
                for box in r.boxes:
                    rows.append({
                        "message_id": message_id,
                        "channel_name": channel_name,
                        "detected_class": r.names[int(box.cls)],
                        "confidence_score": float(box.conf)
                    })

# Always write headers (important)
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "message_id",
            "channel_name",
            "detected_class",
            "confidence_score"
        ]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"YOLO detection complete. {len(rows)} objects detected.")
print(f"Results saved to {OUTPUT_CSV}")
