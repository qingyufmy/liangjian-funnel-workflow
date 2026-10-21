/** Reads only three pre-existing local files, pinned bytes, no production connection. */
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {resolve} from 'node:path';
import {projectFrozenEvidence} from './team_fixture_projection.mjs';
const specs={
  morning:{path:'artifacts/team-v2-iteration-20261020/frozen/morning.json',sha256:'8b869dac2e94ece67b0f83d070b9706b00e399bd5503669a16085aea0c279362'},
  models:{path:'artifacts/team-v2-iteration-20261020/frozen/models.json',sha256:'d898f7e4bd22afb924acf1eab4be4f78c71fb1bd23b4b82b18cf43c946960f11'},
  latest:{path:'artifacts/a4-monitor-20261020/LATEST.json',sha256:'8cae46bd9f84adcd5f790a3bade9ebdcae9016bca8b46dc2361f4270be9b3441'}
};
export async function loadTeamFixture(root){
  if(!root)throw Error('LOCAL_EVIDENCE_ROOT_REQUIRED');
  const evidence={},sources={};
  for(const [key,spec]of Object.entries(specs)){
    const bytes=await readFile(resolve(root,spec.path));
    if(bytes.length>2_000_000)throw Error('LOCAL_EVIDENCE_SIZE_LIMIT');
    const hash=createHash('sha256').update(bytes).digest('hex');
    if(hash!==spec.sha256)throw Error(`LOCAL_EVIDENCE_SHA_MISMATCH:${key}`);
    evidence[key]=JSON.parse(bytes.toString('utf8'));sources[key]={id:key,sha256:hash,bytes:bytes.length};
  }
  return projectFrozenEvidence(evidence,sources);
}
