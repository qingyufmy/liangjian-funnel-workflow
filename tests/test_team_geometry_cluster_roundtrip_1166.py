def test_team_geometry_cluster_actual_publication_roundtrip(tmp_path,monkeypatch):
    from test_b3_publication_end_to_end import current_call,exported
    db,ref,_=current_call(tmp_path,monkeypatch,team=True)
    result=exported(db,ref)
    assert result['scope_complete'],result['reason_codes']
    receipt=result['plans'][0]['publication_projection_receipt']
    assert receipt['evidence_complete'] and receipt['plan_geometry_evidence_complete']
