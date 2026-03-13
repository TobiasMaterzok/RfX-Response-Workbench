import React from "react";
import ReactDOM from "react-dom/client";

import App from "./App";
import CodeModelHelpPage from "./CodeModelHelpPage";

const params = new URLSearchParams(window.location.search);
const showCodeModelHelp =
  params.get("page") === "code-model-help" ||
  window.location.pathname === "/help/code-model";
const RootComponent = showCodeModelHelp ? CodeModelHelpPage : App;

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <RootComponent />
  </React.StrictMode>,
);
