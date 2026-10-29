document.addEventListener("DOMContentLoaded", () => {
  const refresh = async (element) => {
    try {
      const response = await fetch(element.dataset.pollUrl, {headers: {"X-Requested-With": "fetch"}});
      if (response.ok) element.innerHTML = await response.text();
    } catch (_) {
      // A later poll will recover from temporary network failures.
    }
  };

  const list = document.querySelector("#job-list");
  if (list) setInterval(() => {
    fetch("/jobs/fragment").then(r => r.ok ? r.text() : null).then(html => {
      if (html !== null) list.innerHTML = html;
    }).catch(() => {});
  }, 2000);

  const detail = document.querySelector("#job-content");
  if (detail) setInterval(() => refresh(detail), 2000);

  document.querySelectorAll("form[data-confirm]").forEach(form => {
    form.addEventListener("submit", event => {
      if (!window.confirm(form.dataset.confirm)) event.preventDefault();
    });
  });

  const updateEndpoint = (prefix) => {
    const select = document.querySelector(`select[name="${prefix}_type"]`);
    const fields = document.querySelector(`[data-endpoint-fields="${prefix}"]`);
    if (!select || !fields) return;
    const type = select.value;
    fields.querySelector(".imap-fields").hidden = !["imap", "gmail"].includes(type);
    fields.querySelector(".url-fields").hidden = !["jmap", "carddav", "caldav"].includes(type);
    fields.querySelector(".credential-fields").hidden = ["none", "file"].includes(type);
    fields.querySelector(".file-fields").hidden = type !== "file";
    fields.querySelector(".check-field").hidden = type === "none" || type === "file";
  };
  document.querySelectorAll("[data-endpoint-type]").forEach(select => {
    select.addEventListener("change", () => updateEndpoint(select.name.replace(/_type$/, "")));
    updateEndpoint(select.name.replace(/_type$/, ""));
  });

  document.querySelectorAll("[data-preset]").forEach(button => {
    button.addEventListener("click", () => {
      const prefix = button.dataset.target;
      const preset = button.dataset.preset;
      const set = (name, value) => {
        const field = document.querySelector(`[name="${prefix}_${name}"]`);
        if (field) field.value = value;
      };
      if (preset === "gmail") {
        set("type", "gmail"); set("host", "imap.gmail.com"); set("port", "993"); set("security", "ssl");
      } else if (preset === "jmap") {
        set("type", "jmap"); set("url", "https://mail.example.com");
      } else {
        set("type", prefix.startsWith("contacts") ? "carddav" : "caldav");
        set("url", "https://mail.example.com");
      }
      updateEndpoint(prefix);
    });
  });

  document.querySelectorAll(".test-endpoint").forEach(button => {
    button.addEventListener("click", async () => {
      const prefix = `${button.dataset.kind}_${button.dataset.side}`;
      const form = document.querySelector("form");
      const endpoint = {type: value(`${prefix}_type`), host: value(`${prefix}_host`),
        port: Number(value(`${prefix}_port`) || 0), security: value(`${prefix}_security`) || "ssl",
        username: value(`${prefix}_username`), password: value(`${prefix}_password`),
        token: value(`${prefix}_token`), url: value(`${prefix}_url`),
        insecure_tls: Boolean(form.querySelector(`[name="${prefix}_insecure_tls"]`)?.checked),
        files: []};
      const result = button.closest(".endpoint-card").querySelector(".test-result");
      button.disabled = true; result.textContent = "Testing connection…"; result.className = "test-result";
      try {
        const response = await fetch("/api/test", {method: "POST", headers: {"Content-Type": "application/json"},
          body: JSON.stringify({kind: button.dataset.kind, side: button.dataset.side, endpoint})});
        const data = await response.json();
        result.classList.add(data.ok ? "success" : "failure");
        result.textContent = data.detail + (data.folders?.length ? ` · ${data.folders.map(x => x.source).join(", ")}` : "");
      } catch (_) {
        result.classList.add("failure"); result.textContent = "Could not reach the test endpoint.";
      } finally { button.disabled = false; }
    });
  });

  function value(name) { return document.querySelector(`[name="${name}"]`)?.value || ""; }
});
