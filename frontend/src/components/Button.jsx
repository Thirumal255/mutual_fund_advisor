export default function Button({
  variant = "primary",
  children,
  disabled,
  ...props
}) {
  return (
    <button
      className={`btn btn-${variant}`}
      disabled={disabled}
      {...props}
    >
      {children}
    </button>
  );
}
