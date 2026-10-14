"""Decision 19 is a separate quantitative outcome, not a valid LLM reply."""


def rotation_review_failed_twice(history):
    if not isinstance(history, (list, tuple)) or len(history) != 2:
        return False
    return all(isinstance(reasons, (list, tuple)) and reasons
               and all(isinstance(reason, str) and reason.startswith("A2_ROTATION_")
                       for reason in reasons) for reasons in history)
