using Es.Riam.AbstractsOpen;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Logica.Facetado;
using Es.Riam.Gnoss.Logica.Flujos;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.RabbitMQ;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.Web.MVC.Models.Administracion;
using Es.Riam.Gnoss.Web.MVC.Models.Flujos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Gnoss.BackgroundTask.Workflows
{
    internal class ControladorColaFlujos : ControladorServicioGnoss
    {
        private const string COLA_FLUJOS = "ColaFlujos";
        private const string EXCHANGE = "";

        #region Miembros

        private EntityContext mEntityContext;
        private EntityContextBASE mEntityContextBASE;
        private LoggingService mLoggingService;
        private RedisCacheWrapper mRedisCacheWrapper;
        private VirtuosoAD mVirtuosoAD;
        private IServicesUtilVirtuosoAndReplication mServicesUtilVirtuosoAndReplication;
        private GnossCache mGnossCache;

        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;

        #endregion

        #region Constructores

        public ControladorColaFlujos(IServiceScopeFactory scopedFactory, ConfigService configService, ILogger<ControladorColaFlujos> logger, ILoggerFactory loggerFactory)
            : base(scopedFactory, configService, logger, loggerFactory)
        {
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new ControladorColaFlujos(ScopedFactory, mConfigService, mLoggerFactory.CreateLogger<ControladorColaFlujos>(), mLoggerFactory);
        }

        #endregion

        #region Metodos generales

        protected void RealizarMantenimientoRabbitMQ(LoggingService loggingService)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);
                RabbitMQClient rMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_FLUJOS, loggingService, mConfigService, mLoggerFactory.CreateLogger<RabbitMQClient>(), mLoggerFactory);

                try
                {
                    rMQ.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex, $"Error al procesar el elemento de la cola", mlogger);
                    throw;
                }
            }
        }

        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Thread.Sleep(1000);

            GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            parametroAplicacionGBD.ObtenerConfiguracionGnoss(gestorParametroAplicacion);
            mUrlIntragnoss = gestorParametroAplicacion.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;

            FacetaCN facetaCN = new FacetaCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<FacetaCN>(), mLoggerFactory);
            FacetadoAD facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<FacetadoAD>(), mLoggerFactory);
            facetaCN.CargarConfiguracionConexionGrafo(facetadoAD.ServidoresGrafo);
            facetaCN.Dispose();

            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<ProyectoCN>(), mLoggerFactory);
            proyCN.Dispose();

            #region Establezco el dominio de la cache

            GestorParametroAplicacion gestorParametroAplicacionCache = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBDCache = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            parametroAplicacionGBDCache.ObtenerConfiguracionGnoss(gestorParametroAplicacionCache);

            mDominio = gestorParametroAplicacionCache.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;
            mDominio = mDominio.Replace("http://", "").Replace("www.", "");

            if (mDominio[mDominio.Length - 1] == '/')
            {
                mDominio = mDominio.Substring(0, mDominio.Length - 1);
            }
            #endregion

            RealizarMantenimientoRabbitMQ(loggingService);
        }

        public bool ProcesarItem(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                mEntityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                mEntityContextBASE = scope.ServiceProvider.GetService<EntityContextBASE>();
                mLoggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                mVirtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                mRedisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                mServicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                mGnossCache = scope.ServiceProvider.GetService<GnossCache>();

                try
                {
                    ComprobarCancelacionHilo();

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        mUrlIntragnoss = mEntityContext.ParametroAplicacion.Where(parametro => parametro.Parametro.Equals("UrlIntragnoss")).Select(item => item.Valor).FirstOrDefault();

                        ColaProcesarFlujo elementoFila = JsonConvert.DeserializeObject<ColaProcesarFlujo>(pFila);
                        ProcesarFilaDeCola(elementoFila);

                        ControladorConexiones.CerrarConexiones(false);
                    }
                }
                catch (Exception ex)
                {
                    mLoggingService.GuardarLogError(ex, $"Error al procesar la fila --> {pFila}", mlogger);
                }

                return true;
            }
        }

        public void ProcesarFilaDeCola(ColaProcesarFlujo pModel)
        {

            FlujosCN flujosCN = new FlujosCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<FlujosCN>(), mLoggerFactory);
            Dictionary<Guid, Guid> diccionarioRecursoIDEstadoID = new Dictionary<Guid, Guid>();
            switch (pModel.TipoAfectado)
            {
                case TiposContenidos.Nota:
                case TiposContenidos.Adjunto:
                case TiposContenidos.Link:
                case TiposContenidos.Video:
                case TiposContenidos.Debate:
                case TiposContenidos.Encuesta:
                    diccionarioRecursoIDEstadoID = flujosCN.ActualizarEstadosRecursos(pModel.EstadoID, pModel.ProyectoID, new List<Guid>(), (short)pModel.TipoAfectado, pModel.EliminarEstado);
                    break;
                case TiposContenidos.RecursoSemantico:
                    diccionarioRecursoIDEstadoID = flujosCN.ActualizarEstadosRecursos(pModel.EstadoID, pModel.ProyectoID, pModel.OntologiasAfectadas, (short)pModel.TipoAfectado, pModel.EliminarEstado);
                    break;
                case TiposContenidos.PaginaCMS:
                    diccionarioRecursoIDEstadoID = flujosCN.ActualizarEstadoPaginasCMS(pModel.EstadoID, pModel.ProyectoID, pModel.EliminarEstado);
                    break;
                case TiposContenidos.ComponenteCMS:
                    diccionarioRecursoIDEstadoID = flujosCN.ActualizarEstadoComponentesCMS(pModel.EstadoID, pModel.ProyectoID, pModel.EliminarEstado);
                    break;
                default:
                    break;
            }

            ProcesarTriplesEstadosVirtuoso(diccionarioRecursoIDEstadoID, pModel.ProyectoID, pModel.TipoAfectado, !pModel.EliminarEstado);

            if (pModel.EliminarFlujo)
            {
                switch (pModel.TipoAfectado)
                {
                    case TiposContenidos.Nota:
                    case TiposContenidos.Adjunto:
                    case TiposContenidos.Link:
                    case TiposContenidos.Video:
                    case TiposContenidos.Debate:
                    case TiposContenidos.Encuesta:
                    case TiposContenidos.RecursoSemantico:
                        flujosCN.ActualizarEditoresRecursos(diccionarioRecursoIDEstadoID);
                        break;
                }

                List<Guid> estados = flujosCN.ObtenerEstadosIDPorFlujoID(pModel.FlujoID);
                List<Guid> transiciones = flujosCN.ObtenerTransicionesIDPorEstadosID(estados);

                flujosCN.EliminarTransiciones(transiciones);
                flujosCN.EliminarEstados(estados, pModel.FlujoID);
                flujosCN.EliminarFlujo(pModel.FlujoID, pModel.ProyectoID);
            }
        }
        #endregion

        #region Metodos privados

        private void ProcesarTriplesEstadosVirtuoso(Dictionary<Guid, Guid> pDiccionarioRecursos, Guid pProyectoID, TiposContenidos pTipoContenido, bool pAgregarTriples)
        {
            FacetadoCN facetadoCN = new FacetadoCN(mUrlIntragnoss, mEntityContext, mLoggingService, mConfigService, mVirtuosoAD, mServicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<FacetadoCN>(), mLoggerFactory);
            DocumentacionCN documentacionCN = new DocumentacionCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, mLoggerFactory.CreateLogger<DocumentacionCN>(), mLoggerFactory);

            string rdfType = "Recurso";

            HashSet<Guid> recursosProcesados = new HashSet<Guid>();

            if (pAgregarTriples)
            {
                EscribirTriplesEstadoVirtuoso(pDiccionarioRecursos, pProyectoID, pTipoContenido , rdfType, documentacionCN, facetadoCN);
            }
            else
            {
                BorrarTriplesEstadoVirtuoso(pDiccionarioRecursos, pProyectoID, pTipoContenido, rdfType, documentacionCN, facetadoCN);
            }
        }

        private void EscribirTriplesEstadoVirtuoso(Dictionary<Guid, Guid> pDiccionarioRecursos, Guid pProyectoID, TiposContenidos pTipoContenido, string rdfType, DocumentacionCN pDocumentacionCN, FacetadoCN pFacetadoCN)
        {
            HashSet<Guid> recursosProcesados = new HashSet<Guid>();

            foreach (var keyValuePair in pDiccionarioRecursos)
            {
                Guid recursoID = keyValuePair.Key;
                if (pTipoContenido == TiposContenidos.PaginaCMS || pTipoContenido == TiposContenidos.ComponenteCMS)
                {
                    pFacetadoCN.InsertarTripleRdfTypeDeContenido(pProyectoID, recursoID, rdfType);
                }
                else
                {
                    // Si es un recurso (Semantico o no) solo debe insertarse el triple con su ID original
                    recursoID = pDocumentacionCN.ObtenerDocumentoOriginalIDPorID(recursoID);
                }
                if (!recursosProcesados.Contains(recursoID))
                {
                    pFacetadoCN.AnyadirEstadoDeContenido(pProyectoID, keyValuePair.Value, recursoID);
                    recursosProcesados.Add(recursoID);
                }
            }
        }

        private void BorrarTriplesEstadoVirtuoso(Dictionary<Guid, Guid> pDiccionarioRecursos, Guid pProyectoID, TiposContenidos pTipoContenido, string rdfType, DocumentacionCN pDocumentacionCN, FacetadoCN pFacetadoCN)
        {
            HashSet<Guid> recursosProcesados = new HashSet<Guid>();

            foreach (var keyValuePair in pDiccionarioRecursos)
            {
                Guid recursoID = keyValuePair.Key;
                if (pTipoContenido == TiposContenidos.PaginaCMS || pTipoContenido == TiposContenidos.ComponenteCMS)
                {
                    pFacetadoCN.EliminarTripleRdfTypeDeContenido(pProyectoID, recursoID, rdfType);
                }
                else
                {
                    // Si es un recurso (Semantico o no) solo debe insertarse el triple con su ID original
                    recursoID = pDocumentacionCN.ObtenerDocumentoOriginalIDPorID(recursoID);
                }

                if (!recursosProcesados.Contains(recursoID))
                {
                    pFacetadoCN.EliminarEstadoDeContenido(pProyectoID, keyValuePair.Value, recursoID);
                    recursosProcesados.Add(recursoID);
                }
            }
        }

        #endregion
    }
}
