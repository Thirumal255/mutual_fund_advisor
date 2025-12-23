import { adminAction } from "../api/api";
import { useState } from "react";

export default function Admin({ token, onStatusUpdate }) {
  const [output, setOutput] = useState("");
  const [running, setRunning] = useState(false);

  async function run(path) {
    try {
      setRunning(true);
      setOutput("Running...");

      // 1️⃣ Run admin backend command
      const res = await adminAction(token, path);
      setOutput(JSON.stringify(res, null, 2));

      // 2️⃣ IMPORTANT: tell App.jsx to refresh system status
      if (onStatusUpdate) {
        await onStatusUpdate();   // ⭐ THIS IS THE KEY LINE
      }

    } catch (err) {
      setOutput("Error running command");
    } finally {
      setRunning(false);
    }
  }

  return (
    <div style={{ marginTop: 20 }}>
      <h3>Admin Controls</h3>

      <button disabled={running} onClick={() => run("rebuild-masterlist")}>
        Rebuild Masterlist
      </button>

      <button disabled={running} onClick={() => run("extract-sids")}>
        Extract SIDs
      </button>

      <button disabled={running} onClick={() => run("build-metrics")}>
        Build Metrics
      </button>

      <button disabled={running} onClick={() => run("generate-ui")}>
        Generate UI Payload
      </button>

      <pre style={{ background: "#eee", padding: 10, marginTop: 10 }}>
        {output}
      </pre>
    </div>
  );
}
