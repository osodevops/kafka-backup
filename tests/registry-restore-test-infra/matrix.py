#!/usr/bin/env python3
"""Registry/version pairs for the registry restore jobs, as a GitHub Actions matrix.

Default: Confluent 7.6.0 and 8.3.1 with Apicurio 2.6.13.Final and 3.3.3 (both
directions are exercised inside one job), every Apicurio version restored into
itself, and Apicurio API v2 into v3. --full: every ordered pair of the nine
registry versions (81 jobs).
"""
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--full", action="store_true")
args = parser.parse_args()

confluent = ["6.2.0", "7.6.0", "7.9.2", "8.1.1", "8.3.1"]
apicurio = ["2.6.13.Final", "3.0.15", "3.1.7", "3.3.3"]
if args.full:
    providers = [("confluent", v) for v in confluent] + [("apicurio", v) for v in apicurio]
    pairs = [(source, target) for source in providers for target in providers]
else:
    pairs = [(("confluent", c), ("apicurio", a)) for c in ["7.6.0", "8.3.1"] for a in ["2.6.13.Final", "3.3.3"]]
    pairs += [(("apicurio", a), ("apicurio", a)) for a in apicurio]
    pairs += [(("apicurio", "2.6.13.Final"), ("apicurio", "3.3.3"))]

include = []
for (sp, sv), (tp, tv) in pairs:
    include.append({
        "name": f"{sp}-{sv}-to-{tp}-{tv}",
        "source_provider": sp,
        "target_provider": tp,
        "cf_source": sv if sp == "confluent" else (tv if tp == "confluent" else "8.3.1"),
        "cf_target": tv if tp == "confluent" else (sv if sp == "confluent" else "8.3.1"),
        "ap_source": sv if sp == "apicurio" else (tv if tp == "apicurio" else "3.3.3"),
        "ap_target": tv if tp == "apicurio" else (sv if sp == "apicurio" else "3.3.3"),
    })
print(json.dumps({"include": include}, separators=(",", ":")))
