export default function Card({ title, actions, children }) {
  return (
    <div className="card">
      {(title || actions) && (
        <div className="card-header">
          {title && <h3>{title}</h3>}
          {actions}
        </div>
      )}
      <div className="card-body">{children}</div>
    </div>
  );
}
