import type { ThemePreference } from "./theme";

type ThemeToggleProps = {
  preference: ThemePreference;
  onChange: (nextPreference: ThemePreference) => void;
  className?: string;
};

const THEME_OPTIONS: Array<{ label: string; value: ThemePreference }> = [
  { label: "Auto", value: "system" },
  { label: "Light", value: "light" },
  { label: "Dark", value: "dark" },
];

function ThemeToggle({ preference, onChange, className }: ThemeToggleProps) {
  return (
    <div className={className ? `theme-toggle ${className}` : "theme-toggle"}>
      <span className="theme-toggle-label">Theme</span>
      <div
        className="theme-toggle-group"
        role="group"
        aria-label="Theme preference"
      >
        {THEME_OPTIONS.map((option) => (
          <button
            key={option.value}
            type="button"
            className={
              preference === option.value
                ? "theme-toggle-button active"
                : "theme-toggle-button"
            }
            aria-pressed={preference === option.value}
            onClick={() => onChange(option.value)}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

export default ThemeToggle;
