import React from "react";
import { createRoot } from "react-dom/client";
import { StageDetailDialog } from "../../web/src/App";
import "../../web/src/styles.css";

const target = { runId: "fixture", laneId: "lane_1", model: "冻结数据验收（不连接生产）",
  stage: { stage: "A3", status: "VALIDATED", symbolCount: 26 } } as React.ComponentProps<typeof StageDetailDialog>["target"];
createRoot(document.getElementById("root")!).render(<><p>9月7日冻结记录界面验收；不会生成计划或连接生产。</p><StageDetailDialog target={target} onDismiss={() => undefined} /></>);
