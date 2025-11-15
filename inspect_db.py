from mls_app import db, Listing, Field, Observation, app

with app.app_context():
    print("Listings:", Listing.query.count())
    print("Fields:", Field.query.count())
    print("Observations:", Observation.query.count())
    print("=== sample listings ===")
    for l in Listing.query.limit(10).all():
        print("L:", l.id, l.listing_id_text, l.batch)
    print("=== sample fields ===")
    for f in Field.query.limit(10).all():
        print("F:", f.id, f.canonical)
    print("=== sample observations (first 20) ===")
    for o in Observation.query.limit(20).all():
        print("O:", o.id, "listing_id:", o.listing_id, "field_id:", o.field_id, "filled:", o.filled, "raw:", o.raw_text)
