def validate_diff(diff, allow_type_widening=True, forbid_column_removal=True):
    errors = []

    if forbid_column_removal and diff["remove"]:
        errors.append("Column removal is not allowed")

    for tc in diff["type_changes"]:
        if not allow_type_widening and tc["from"] != tc["to"]:
            errors.append(f"Type change not allowed: {tc['name']}")

    return errors
