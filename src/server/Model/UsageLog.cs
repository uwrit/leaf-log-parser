using System;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Text;

namespace Model
{
    public class LogRecord
    {
        public DateTime Timestamp { get; set; }
        public string Level { get; set; }
        public string MessageTemplate { get; set; }
        public object Properties { get; set; }
        public object Renderings { get; set; }

        public LogEntry ToLogEntry()
        {
            var entry = new LogEntry
            {
                Timestamp = Timestamp,
                Level = Level,
                MessageTemplate = MessageTemplate,
                Properties = Properties.ToString(),
                Renderings = Renderings == null ? null : Renderings.ToString()
            };
            entry.LiftSubProperties();
            return entry;
        }
    }

    public class LogEntry
    {
        public DateTime Timestamp { get; set; }
        public string Level { get; set; }
        public string MessageTemplate { get; set; }
        public string Properties { get; set; }
        public string Renderings { get; set; }
        public string ActionId { get; set; }
        public string ActionName { get; set; }
        public string ConnectionId { get; set; }
        public string RequestId { get; set; }
        public string RequestPath { get; set; }
        public string SessionId { get; set; }
        public string SourceContext { get; set; }
        public string User { get; set; }

        public void LiftSubProperties()
        {
            var parsed = JObject.Parse(Properties);

            var actionId = parsed[Props.ActionId];
            var actionName = parsed[Props.ActionName];
            var connId = parsed[Props.ConnectionId];
            var reqId = parsed[Props.RequestId];
            var reqPath = parsed[Props.RequestPath];
            var sessId = parsed[Props.SessionId];
            var sourceCtx = parsed[Props.SourceContext];
            var user = parsed[Props.User];

            if (actionId != null)
            {
                ActionId = actionId.ToString();
            }
            if (actionName != null)
            {
                ActionName = actionName.ToString();
            }
            if (connId != null)
            {
                ConnectionId = connId.ToString();
            }
            if (reqId != null)
            {
                RequestId = reqId.ToString();
            }
            if (reqPath != null)
            {
                RequestPath = reqPath.ToString();
            }
            if (sessId != null)
            {
                SessionId = sessId.ToString();
            }
            if (sourceCtx != null)
            {
                SourceContext = sourceCtx.ToString();
            }
            if (user != null)
            {
                User = user.ToString();
            }
        }
    }

    static class Props
    {
        public const string Timestamp = "Timestamp";
        public const string Level = "Level";
        public const string MessageTemplate = "MessageTemplate";
        public const string Properties = "Properties";
        public const string Renderings = "Renderings";
        public const string ActionId = "ActionId";
        public const string ActionName = "ActionName";
        public const string ConnectionId = "ConnectionId";
        public const string RequestId = "RequestId";
        public const string RequestPath = "RequestPath";
        public const string SessionId = "SessionId";
        public const string SourceContext = "SourceContext";
        public const string User = "User";
    }
}
