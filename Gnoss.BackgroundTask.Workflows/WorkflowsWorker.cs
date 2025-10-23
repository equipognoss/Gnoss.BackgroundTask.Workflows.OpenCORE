using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Gnoss.BackgroundTask.Workflows
{
    
    public class WorkflowsWorker: Worker
    {
        private readonly ConfigService _configService;
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;
        public WorkflowsWorker(ConfigService configService, IServiceScopeFactory scopeFactory, ILogger<WorkflowsWorker> mlogger, ILoggerFactory mLoggerFactory)
            :base(mlogger, scopeFactory)
        {
            _configService = configService;
            this.mlogger = mlogger;
            this.mLoggerFactory = mLoggerFactory;
        }
        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new ControladorColaFlujos(ScopedFactory, _configService, mLoggerFactory.CreateLogger<ControladorColaFlujos>(), mLoggerFactory));
            return controladores;
        }
    }
}
