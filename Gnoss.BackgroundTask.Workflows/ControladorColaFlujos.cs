using Es.Riam.AbstractsOpen;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.AD.Live;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.CL.CMS;
using Es.Riam.Gnoss.Elementos.CMS;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.Logica.CMS;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Logica.Facetado;
using Es.Riam.Gnoss.Logica.Flujos;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.RabbitMQ;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Web.Controles.Documentacion;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.Web.MVC.Models.Administracion;
using Es.Riam.Gnoss.Web.MVC.Models.Flujos;
using Es.Riam.Interfaces.InterfacesOpen;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Gnoss.BackgroundTask.Workflows
{
    internal class ControladorColaFlujos : ControladorServicioGnoss
    {
        private const string COLA_FLUJOS = "ColaFlujos";

        #region Miembros

        private EntityContext mEntityContext;
        private EntityContextBASE mEntityContextBASE;
        private LoggingService mLoggingService;
        private RedisCacheWrapper mRedisCacheWrapper;
        private VirtuosoAD mVirtuosoAD;
        private IServicesUtilVirtuosoAndReplication mServicesUtilVirtuosoAndReplication;
        private GnossCache mGnossCache;
        private IAvailableServices mAvailableServices;

        private readonly ILogger _logger;
        private readonly ILoggerFactory _loggerFactory;

        #endregion

        #region Constructores

        public ControladorColaFlujos(IServiceScopeFactory scopedFactory, ConfigService configService, ILogger<ControladorColaFlujos> logger, ILoggerFactory loggerFactory)
            : base(scopedFactory, configService, logger, loggerFactory)
        {
            _logger = logger;
            _loggerFactory = loggerFactory;
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new ControladorColaFlujos(ScopedFactory, mConfigService, _loggerFactory.CreateLogger<ControladorColaFlujos>(), _loggerFactory);
        }

        #endregion

        #region Metodos generales

        protected void RealizarMantenimientoRabbitMQ(LoggingService loggingService)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);
                RabbitMQClient rMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_FLUJOS, loggingService, mConfigService, _loggerFactory.CreateLogger<RabbitMQClient>(), _loggerFactory);

                try
                {
                    rMQ.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex, $"Error al procesar el elemento de la cola", _logger);
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

            FacetaCN facetaCN = new FacetaCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<FacetaCN>(), _loggerFactory);
            FacetadoAD facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<FacetadoAD>(), _loggerFactory);
            facetaCN.CargarConfiguracionConexionGrafo(facetadoAD.ServidoresGrafo);
            facetaCN.Dispose();

            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<ProyectoCN>(), _loggerFactory);
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
                mAvailableServices = scope.ServiceProvider.GetRequiredService<IAvailableServices>();

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
                    mLoggingService.GuardarLogError(ex, $"Error al procesar la fila --> {pFila}", _logger);
                }

                return true;
            }
        }

        public void ProcesarFilaDeCola(ColaProcesarFlujo pModel)
        {
            FlujosCN flujosCN = new FlujosCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<FlujosCN>(), _loggerFactory);
            try
            {
                flujosCN.IniciarTransaccion();
                Dictionary<Guid, Guid> diccionarioRecursoIDEstadoID = new Dictionary<Guid, Guid>();
                foreach(TiposContenidos tipoContenido in pModel.TiposAfectados)
                {
                    switch (tipoContenido)
                    {
                        case TiposContenidos.Nota:
                        case TiposContenidos.Adjunto:
                        case TiposContenidos.Link:
                        case TiposContenidos.Video:
                        case TiposContenidos.Debate:
                        case TiposContenidos.Encuesta:
                        case TiposContenidos.RecursoSemantico:
                            List<Guid> ontlogiasABuscar = tipoContenido == TiposContenidos.RecursoSemantico ? pModel.OntologiasAfectadas : new List<Guid>();
                            diccionarioRecursoIDEstadoID = flujosCN.ActualizarEstadosRecursos(pModel.EstadoID, pModel.ProyectoID, ontlogiasABuscar, (short)tipoContenido, pModel.EliminarEstado);
                            // Actualizar los estadoid de las versiones 
                            flujosCN.ActualizarEstadosVersionRecursos(diccionarioRecursoIDEstadoID.Keys.ToList(), pModel.EstadoID, pModel.EliminarEstado);
                            // Si se elimina el flujo, asignar editores a los recursos en caso de que no tengna ninguno disponible
                            if (pModel.EliminarFlujo)
                            {
                                flujosCN.ActualizarEditoresRecursos(diccionarioRecursoIDEstadoID);
                            }
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

                    ProcesarTriplesEstadosVirtuoso(diccionarioRecursoIDEstadoID, pModel.ProyectoID, tipoContenido, !pModel.EliminarEstado);
                    InvalidarCaches(diccionarioRecursoIDEstadoID, pModel.ProyectoID, pModel.UsuarioID, tipoContenido);
                }

                if (pModel.EliminarFlujo)
                {
                    EliminarFlujo(pModel.FlujoID, pModel.ProyectoID, flujosCN);
                }

                flujosCN.TerminarTransaccion(true);
            }
            catch (Exception ex)
            {
                flujosCN.TerminarTransaccion(false);
                mLoggingService.GuardarLogError(ex, $"ERROR: ${ex.Message}\r\nStackTrace: {ex.StackTrace}", _logger);
            }
            finally
            {
                flujosCN.Dispose();
            }
        }
        #endregion

        #region Metodos privados

        private void ProcesarTriplesEstadosVirtuoso(Dictionary<Guid, Guid> pDiccionarioRecursos, Guid pProyectoID, TiposContenidos pTipoContenido, bool pAgregarTriples)
        {
            FacetadoCN facetadoCN = new FacetadoCN(mUrlIntragnoss, mEntityContext, mLoggingService, mConfigService, mVirtuosoAD, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<FacetadoCN>(), _loggerFactory);
            DocumentacionCN documentacionCN = new DocumentacionCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<DocumentacionCN>(), _loggerFactory);

            string rdfType = "Recurso";

            if (pTipoContenido == TiposContenidos.PaginaCMS)
            {
                rdfType = FacetadoAD.PAGINA_CMS;
            }
            else if (pTipoContenido == TiposContenidos.ComponenteCMS)
            {
                rdfType = FacetadoAD.COMPONENTE_CMS;
            }

            if (pAgregarTriples)
            {
                EscribirTriplesEstadoVirtuoso(pDiccionarioRecursos, pProyectoID, pTipoContenido, rdfType, documentacionCN, facetadoCN);
            }
            else
            {
                BorrarTriplesEstadoVirtuoso(pDiccionarioRecursos, pProyectoID, pTipoContenido, rdfType, documentacionCN, facetadoCN);
            }

            facetadoCN.Dispose();
            documentacionCN.Dispose();
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

        private void InvalidarCaches(Dictionary<Guid, Guid> pDiccionarioRecursos, Guid pProyectoID, Guid pUsuarioID, TiposContenidos pTipoContenido)
        {
            mLoggingService.AgregarEntrada($"Invalidando cachés: \t\nTipo de recurso: {pTipoContenido}\t\nLista de recursos: {string.Join(",", pDiccionarioRecursos.Keys)}\t\nProyectoID: {pProyectoID}\t\nUsuarioID: {pUsuarioID}");

            switch (pTipoContenido)
            {
                case TiposContenidos.Nota:
                case TiposContenidos.Adjunto:
                case TiposContenidos.Link:
                case TiposContenidos.Video:
                case TiposContenidos.Debate:
                case TiposContenidos.Encuesta:
                case TiposContenidos.RecursoSemantico:
                    ControladorDocumentacion controladorDocumentacion = new ControladorDocumentacion(mLoggingService, mEntityContext, mConfigService, mRedisCacheWrapper, mGnossCache, mEntityContextBASE, mVirtuosoAD, null, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<ControladorDocumentacion>(), _loggerFactory);
                    DocumentacionCN docCN = new DocumentacionCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<DocumentacionCN>(), _loggerFactory);
                    FlujosCN flujosCN = new FlujosCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<FlujosCN>(), _loggerFactory);

                    foreach (Guid documentoID in pDiccionarioRecursos.Keys)
                    {
                        controladorDocumentacion.BorrarCacheControlFichaRecursos(documentoID);

                        #region Actualizar Live
                        bool estadoPublico = flujosCN.ComprobarEstadoEsPublico(pDiccionarioRecursos[documentoID]);
                        bool recursoPublico = !docCN.EsDocumentoEnProyectoPrivadoEditores(documentoID, pProyectoID);
                        bool privacidadCambiada = recursoPublico != estadoPublico;
                        if (docCN.ComprobarSiEsUltimaVersionDocumento(documentoID))
                        {
                            ActualizarLive(pProyectoID, documentoID, pTipoContenido, privacidadCambiada, controladorDocumentacion);
                        }
                        #endregion
                    }
                    flujosCN.Dispose();
                    docCN.Dispose();
                    break;
                case TiposContenidos.PaginaCMS:
                    using (CMSCL cmsCL = new CMSCL(mEntityContext, mLoggingService, mRedisCacheWrapper, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<CMSCL>(), _loggerFactory))
                    {
                        cmsCL.InvalidarCacheQueContengaCadena(pProyectoID.ToString());
                    }
                    break;
                case TiposContenidos.ComponenteCMS:
                    using (CMSCL cmsCL = new CMSCL(mEntityContext, mLoggingService, mRedisCacheWrapper, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<CMSCL>(), _loggerFactory))
                    {
                        cmsCL.InvalidarCachesDeComponentesEnProyecto(pProyectoID);
                        cmsCL.InvalidarCacheConfiguracionCMSPorProyecto(pProyectoID);
                        using (CMSCN CMSCN = new CMSCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<CMSCN>(), _loggerFactory))
                        using (GestionCMS gestorCMS2 = new GestionCMS(CMSCN.ObtenerCMSDeProyecto(pProyectoID), mLoggingService, mEntityContext))
                        {
                            if (gestorCMS2.ListaPaginasProyectos.ContainsKey(pProyectoID))
                            {
                                foreach (short tipoPagina in gestorCMS2.ListaPaginasProyectos[pProyectoID].Keys)
                                {
                                    cmsCL.InvalidarCacheCMSDeUbicacionDeProyecto(tipoPagina, pProyectoID);
                                }

                                ProyectoCN proyCN = new ProyectoCN(mEntityContext, mLoggingService, mConfigService, mServicesUtilVirtuosoAndReplication, _loggerFactory.CreateLogger<ProyectoCN>(), _loggerFactory);
                                List<Guid> proys = new List<Guid> { pProyectoID };
                                DataWrapperProyecto dw = proyCN.ObtenerProyectosHijosDeProyectos(proys, pUsuarioID);
                                cmsCL.InvalidarCachesCMSDeUbicacionesDeProyectos(dw.ListaProyecto);
                                proyCN.Dispose();
                            }
                        }
                    }
                    break;
            }
        }

        private void ActualizarLive(Guid pProyectoID, Guid pDocumentoID, TiposContenidos pTipoContenido, bool pPrivacidadCambiada, ControladorDocumentacion pControladorDocumentacion)
        {
            int tipoLive = ObtenerTipoLive(pTipoContenido);

            string infoExtra = pPrivacidadCambiada ? Constantes.PRIVACIDAD_CAMBIADA : string.Empty;

            pControladorDocumentacion.ActualizarGnossLive(pProyectoID, pDocumentoID, AccionLive.Agregado, tipoLive, PrioridadLive.Media, infoExtra, mAvailableServices);

            if (pPrivacidadCambiada)
            {
                pControladorDocumentacion.ActualizarGnossLive(pProyectoID, pDocumentoID, AccionLive.Editado, tipoLive, PrioridadLive.Media, infoExtra, mAvailableServices);
            }
        }

        private int ObtenerTipoLive(TiposContenidos pTipoContenido)
        {
            return pTipoContenido == TiposContenidos.Debate ? (int)TipoLive.Debate : (int)TipoLive.Recurso;
        }

        private void EliminarFlujo(Guid pFlujoID, Guid pProyectoID, FlujosCN pFlujosCN)
        {
            List<Guid> estados = pFlujosCN.ObtenerEstadosIDPorFlujoID(pFlujoID);
            List<Guid> transiciones = pFlujosCN.ObtenerTransicionesIDPorEstadosID(estados);

            pFlujosCN.EliminarTransiciones(transiciones);
            pFlujosCN.EliminarEstados(estados, pFlujoID);
            pFlujosCN.EliminarFlujo(pFlujoID, pProyectoID);
        }

        #endregion
    }
}
