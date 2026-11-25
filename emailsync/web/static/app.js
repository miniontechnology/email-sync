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

  document.addEventListener("submit", event => {
    const form = event.target.closest("form[data-confirm]");
    if (form && !window.confirm(form.dataset.confirm)) event.preventDefault();
  });

  const updateEndpoint = (prefix) => {
    const select = document.querySelector(`select[name="${prefix}_type"]`);
    const fields = document.querySelector(`[data-endpoint-fields="${prefix}"]`);
    if (!select || !fields) return;
    const type = select.value;
    const saved = document.querySelector(`[name="${prefix}_account_id"]`)?.value;
    fields.hidden = Boolean(saved);
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
  document.querySelectorAll("[data-saved-account]").forEach(select => {
    select.addEventListener("change", () => updateEndpoint(select.name.replace(/_account_id$/, "")));
    updateEndpoint(select.name.replace(/_account_id$/, ""));
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

  const endpointFromForm = (form, prefix) => {
    const type = value(`${prefix}_type`);
    if (!type || type === "none") return null;
    return {type, host: value(`${prefix}_host`),
      account_id: Number(value(`${prefix}_account_id`) || 0) || null,
      port: Number(value(`${prefix}_port`) || 0), security: value(`${prefix}_security`) || "ssl",
      username: value(`${prefix}_username`), password: value(`${prefix}_password`),
      token: value(`${prefix}_token`), url: value(`${prefix}_url`),
      insecure_tls: Boolean(form.querySelector(`[name="${prefix}_insecure_tls"]`)?.checked),
      files: []};
  };

  const folderOptionsFromForm = form => {
    const folderMap = {};
    const mapText = form.querySelector('[name="folder_map"]')?.value || "";
    mapText.split(/\r?\n/).forEach(line => {
      if (line.trim().startsWith("#")) return;
      const separator = line.indexOf("=>");
      if (separator < 0) return;
      const key = line.slice(0, separator).trim();
      if (key) folderMap[key] = line.slice(separator + 2).trim();
    });
    const excluded = form.querySelector('[name="exclude_folders"]')?.value || "";
    const checked = name => Boolean(form.querySelector(`[name="${name}"]`)?.checked);
    return {
      folder_map: folderMap,
      exclude_folders: excluded.split(/\r?\n/).map(line => line.trim()).filter(Boolean),
      include_spam: checked("include_spam"),
      include_trash: checked("include_trash"),
      gmail_important_as_folder: checked("gmail_important_as_folder"),
      archive_target: value("archive_target") || "@archive"
    };
  };

  document.querySelectorAll(".test-endpoint").forEach(button => {
    button.addEventListener("click", async () => {
      const prefix = `${button.dataset.kind}_${button.dataset.side}`;
      const form = document.querySelector("form");
      const endpoint = endpointFromForm(form, prefix);
      const result = button.closest(".endpoint-card").querySelector(".test-result");
      button.disabled = true; result.textContent = "Testing connection…"; result.className = "test-result";
      try {
        const response = await fetch("/api/test", {method: "POST", headers: {"Content-Type": "application/json"},
          body: JSON.stringify({kind: button.dataset.kind, side: button.dataset.side, endpoint,
            options: folderOptionsFromForm(form), job_id: form.dataset.jobId || null})});
        const data = await response.json();
        result.classList.add(data.ok ? "success" : "failure");
        result.textContent = data.detail + (data.folders?.length ? ` · ${data.folders.map(x => x.source).join(", ")}` : "");
      } catch (_) {
        result.classList.add("failure"); result.textContent = "Could not reach the test endpoint.";
      } finally { button.disabled = false; }
    });
  });

  const picker = document.querySelector("#folder-picker");
  if (picker) {
    const form = picker.closest("form");
    const loadButton = picker.querySelector("#load-folders");
    const status = picker.querySelector("#folder-picker-status");
    const destStatus = picker.querySelector("#folder-picker-dest-status");
    const result = picker.querySelector("#folder-picker-result");
    const datalist = picker.querySelector("#dest-folder-options");
    const gmailNote = picker.querySelector("#folder-picker-gmail-note");
    const folderMap = form.querySelector('[name="folder_map"]');
    const excludeFolders = form.querySelector('[name="exclude_folders"]');
    let sourceRows = null;
    let destFolders = [];

    const parseMap = () => {
      const mapping = new Map();
      (folderMap.value || "").split(/\r?\n/).forEach(line => {
        if (line.trim().startsWith("#")) return;
        const separator = line.indexOf("=>");
        if (separator < 0) return;
        const key = line.slice(0, separator).trim();
        if (key) mapping.set(key, line.slice(separator + 2).trim());
      });
      return mapping;
    };
    const parseExcluded = () => new Set(
      (excludeFolders.value || "").split(/\r?\n/).map(line => line.trim()).filter(Boolean)
    );
    const checkbox = name => form.querySelector(`[name="${name}"]`);
    const defaultPlaceholder = target => ({
      "@inbox": "Inbox",
      "@sent": "Sent",
      "@drafts": "Drafts",
      "@trash": "Trash",
      "@junk": "Junk",
      "@archive": "Archive"
    })[target] || target || "";

    const syncTextareas = () => {
      if (!sourceRows) return;
      const rowElements = [...result.querySelectorAll("tr[data-folder-key]")];
      const rowKeys = new Set(rowElements.map(row => row.dataset.folderKey));
      const excludes = rowElements
        .filter(row => row.dataset.folderKind === "normal" && !row.querySelector("input[type=checkbox]").checked)
        .map(row => row.dataset.folderKey);
      for (const key of parseExcluded()) {
        if (!rowKeys.has(key)) excludes.push(key);
      }
      excludeFolders.value = [...new Set(excludes)].join("\n");

      const mappings = rowElements
        .filter(row => row.querySelector("input[type=checkbox]").checked)
        .map(row => ({
          key: row.dataset.folderKey,
          dest: row.querySelector("input[type=text]").value.trim()
        }))
        .filter(row => row.dest);
      for (const [key, dest] of parseMap()) {
        if (!rowKeys.has(key)) mappings.push({key, dest});
      }
      folderMap.value = mappings.map(row => `${row.key} => ${row.dest}`).join("\n");
    };

    const renderRows = () => {
      gmailNote.hidden = value("mail_source_type") !== "gmail";
      if (!sourceRows) return;
      const mapping = parseMap();
      const excluded = parseExcluded();
      const rows = [...sourceRows];
      const sourceKeys = new Set(sourceRows.map(row => row.key));
      for (const key of new Set([...mapping.keys(), ...excluded])) {
        if (!sourceKeys.has(key)) {
          rows.push({
            key, name: key, kind: "normal", default_target: "",
            included: !excluded.has(key), dest: mapping.get(key) || "", missing: true
          });
        }
      }
      datalist.replaceChildren(...destFolders.map(name => {
        const option = document.createElement("option");
        option.value = name;
        return option;
      }));
      result.replaceChildren();
      const table = document.createElement("table");
      table.className = "folder-picker-table";
      table.innerHTML = "<thead><tr><th>Include</th><th>Source folder</th><th>Destination</th></tr></thead>";
      const body = document.createElement("tbody");
      for (const row of rows) {
        const tr = document.createElement("tr");
        tr.dataset.folderKey = row.key;
        tr.dataset.folderKind = row.kind;

        const includeCell = document.createElement("td");
        const includeLabel = document.createElement("label");
        includeLabel.className = "check-field folder-include";
        const includeBox = document.createElement("input");
        includeBox.type = "checkbox";
        includeBox.checked = row.kind === "spam"
          ? Boolean(checkbox("include_spam")?.checked)
          : row.kind === "trash"
            ? Boolean(checkbox("include_trash")?.checked)
            : row.kind === "important"
              ? Boolean(checkbox("gmail_important_as_folder")?.checked)
                && !(mapping.has(row.key) && !mapping.get(row.key))
              : !excluded.has(row.key) && !(mapping.has(row.key) && !mapping.get(row.key));
        includeBox.setAttribute("aria-label", `Include ${row.name}`);
        includeLabel.append(includeBox);
        includeCell.append(includeLabel);

        const sourceCell = document.createElement("td");
        const sourceName = document.createElement("span");
        sourceName.className = "scope-name";
        sourceName.textContent = row.name;
        sourceCell.append(sourceName);
        if (row.missing) {
          const missing = document.createElement("span");
          missing.className = "folder-missing-tag";
          missing.textContent = "not found on source";
          sourceCell.append(missing);
        }

        const destCell = document.createElement("td");
        const destInput = document.createElement("input");
        destInput.type = "text";
        destInput.setAttribute("list", "dest-folder-options");
        destInput.setAttribute("aria-label", `Destination for ${row.name}`);
        destInput.placeholder = defaultPlaceholder(row.default_target);
        destInput.value = mapping.get(row.key) || "";
        destCell.append(destInput);
        tr.append(includeCell, sourceCell, destCell);
        body.append(tr);

        includeBox.addEventListener("change", () => {
          if (row.kind === "spam") checkbox("include_spam").checked = includeBox.checked;
          if (row.kind === "trash") checkbox("include_trash").checked = includeBox.checked;
          if (row.kind === "important") checkbox("gmail_important_as_folder").checked = includeBox.checked;
          syncTextareas();
        });
        destInput.addEventListener("input", syncTextareas);
      }
      table.append(body);
      result.append(table);
    };

    loadButton.addEventListener("click", async () => {
      loadButton.disabled = true;
      status.className = "test-result";
      destStatus.className = "test-result";
      status.textContent = "Loading source folders…";
      destStatus.textContent = "";
      try {
        const response = await fetch("/api/folders", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            source: endpointFromForm(form, "mail_source"),
            dest: endpointFromForm(form, "mail_dest"),
            job_id: form.dataset.jobId || null,
            options: folderOptionsFromForm(form)
          })
        });
        const data = await response.json();
        status.textContent = data.detail || "Could not load source folders.";
        status.classList.add(data.ok ? "success" : "failure");
        destStatus.textContent = data.dest_detail || (
          data.dest_folders?.length ? `Loaded ${data.dest_folders.length} destination folders.` : ""
        );
        if (data.dest_detail) destStatus.classList.add("failure");
        sourceRows = data.ok ? (data.rows || []).filter(row => !row.missing) : null;
        destFolders = data.dest_folders || [];
        if (!data.ok) result.replaceChildren();
        renderRows();
      } catch (_) {
        sourceRows = null;
        result.replaceChildren();
        status.className = "test-result failure";
        status.textContent = "Could not reach the folder service.";
        destStatus.textContent = "";
      } finally {
        loadButton.disabled = false;
      }
    });

    folderMap.addEventListener("input", renderRows);
    excludeFolders.addEventListener("input", renderRows);
    ["include_spam", "include_trash", "gmail_important_as_folder"].forEach(name => {
      checkbox(name).addEventListener("change", () => {
        renderRows();
        syncTextareas();
      });
    });
    form.querySelector('[name="mail_source_type"]').addEventListener("change", renderRows);
    renderRows();
  }

  function value(name) { return document.querySelector(`[name="${name}"]`)?.value || ""; }
});
