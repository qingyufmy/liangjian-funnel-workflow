"""Display the existing runtime cluster; never change its resolution order."""
from copy import deepcopy
import json
from ..review.observability import _cluster

FIELD='cluster_display_projection'


def project_cluster_display(payload):
    result=deepcopy(dict(payload))
    cluster,basis=_cluster({'payload_json':json.dumps(result,ensure_ascii=False)})
    result['cluster']=cluster
    result[FIELD]=dict(version='runtime-cluster-display/1',cluster=cluster,basis=basis,
        authority='DISPLAY_ONLY_EXISTING_RUNTIME_RESOLVER')
    return result
