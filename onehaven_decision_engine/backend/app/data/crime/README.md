# Crime data folder

This folder is intentionally local-data-first.

Supported optional files:

- `crime_points.csv`
- `crime_index.csv`
- `crime_points.json`
- `crime_index.json`

## Expected columns / keys

Minimum required:

- `lat`
- `lng`

Optional:

- `weight`
- `category`

## CSV example

```csv
lat,lng,weight,category
42.3318,-83.0458,1.0,property
42.3470,-83.0582,2.0,violent
JSON example
[
  { "lat": 42.3318, "lng": -83.0458, "weight": 1.0, "category": "property" },
  { "lat": 42.3470, "lng": -83.0582, "weight": 2.0, "category": "violent" }
]
Behavior when no dataset exists

The application falls back to a deterministic heuristic based on county, city, red-zone membership, and light coordinate variation. That keeps development and tests stable without pretending fake precision is real precision.


---

## `onehaven_decision_engine/backend/app/data/offenders/README.md`

```md
# Offender data folder

This folder stores optional local offender registry points used by `offender_index.py`.

Supported optional files:

- `offenders.csv`
- `registry_points.csv`
- `offenders.json`
- `registry_points.json`

## Expected columns / keys

Minimum required:

- `lat`
- `lng`

## CSV example

```csv
lat,lng
42.3331,-83.0552
42.3410,-83.0729
JSON example
[
  { "lat": 42.3331, "lng": -83.0552 },
  { "lat": 42.3410, "lng": -83.0729 }
]
Notes

The service counts points within a default 1-mile radius and also returns the nearest known point when dataset-backed results are available.

If no dataset is present, the app uses a deterministic fallback heuristic so tests and local development still behave consistently.

