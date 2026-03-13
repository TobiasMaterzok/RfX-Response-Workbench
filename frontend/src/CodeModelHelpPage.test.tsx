import { fireEvent, render, screen } from "@testing-library/react";

import CodeModelHelpPage from "./CodeModelHelpPage";

describe("CodeModelHelpPage", () => {
  it("renders the conceptual flow and lets the user inspect nodes", () => {
    render(<CodeModelHelpPage />);

    expect(
      screen.getByRole("heading", { name: "Conceptual Code Model" }),
    ).toBeInTheDocument();
    expect(screen.getByText(/scoped source artifacts/i)).toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", {
        name: /Execution runs and model invocations are first-class artifacts/i,
      }),
    );

    expect(
      screen.getByRole("heading", {
        name: "Execution runs and model invocations are first-class artifacts",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/strict-eval mode enforces consistency checks/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText("backend/app/services/reproducibility.py"),
    ).toBeInTheDocument();
  });
});
