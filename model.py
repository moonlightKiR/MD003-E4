import h2o
from h2o.frame import H2OFrame
from h2o.estimators import H2ORandomForestEstimator, H2OGradientBoostingEstimator
from h2o.estimators import H2OPrincipalComponentAnalysisEstimator

CLASSIFICATION_VAR = "success"
REGRESSION_VAR = "targtype1_txt"

def init(df):
    h2o.init()
    hf = H2OFrame(df)
    return hf

def split_data(hf):
    # Columnas predictoras
    predictors = [col for col in hf.columns if col not in [CLASSIFICATION_VAR, REGRESSION_VAR]]

    # Target para clasificación
    classification_target = CLASSIFICATION_VAR
    hf[classification_target] = hf[classification_target].asfactor()  # convertido a factor

    # Target para regresión
    regression_target = REGRESSION_VAR
    hf[regression_target] = hf[regression_target].asnumeric()  # aseguro que sea numérico

    return predictors, classification_target, regression_target, hf

def divide_data(hf):
    train, test = hf.split_frame(ratios=[0.8], seed=1234)
    return train, test

def plot_model_results(model, perf):
    """Genera visualizaciones clave del modelo."""
    print("\n--- Generando Visualizaciones de H2O ---")
    
    # 1. Importancia de Variables (¿Qué es lo más relevante?)
    try:
        print("Graficando Importancia de Variables...")
        model.varimp_plot()
    except:
        print("No se pudo generar el gráfico de importancia.")

    # 2. Curva ROC (Solo si es clasificación)
    try:
        if hasattr(perf, "roc"):
            print("Graficando Curva ROC...")
            perf.plot(type="roc")
    except:
        pass

def classify_h2o(train, test, predictors, classification_target):
    # Crear y entrenar clasificacion
    rf_clf = H2ORandomForestEstimator(
        ntrees=50,
        max_depth=20,
        seed=1234,
        balance_classes=True
    )

    rf_clf.train(x=predictors, y=classification_target, training_frame=train)

    # Evaluar test
    perf_clf_test = rf_clf.model_performance(test_data=test)

    # Accuracy y F1
    accuracy_default = perf_clf_test.accuracy()[0][1]
    best_f1_threshold = perf_clf_test.F1()[0][0]
    accuracy_best = perf_clf_test.accuracy(thresholds=[best_f1_threshold])[0][1]

    print(f"Accuracy (default threshold): {accuracy_default:.4f}")
    print(f"Accuracy (threshold max F1): {accuracy_best:.4f}")
    print(f"AUC: {perf_clf_test.auc():.4f}")
    print("F1:", perf_clf_test.F1())
    print("Confusion Matrix:")
    print(perf_clf_test.confusion_matrix())
    
    # Llamamos a las visualizaciones
    plot_model_results(rf_clf, perf_clf_test)
    
    return rf_clf

def regression_h2o(train, test, predictors, regression_target):
    # Crear y entrenar regresión
    rf_reg = H2ORandomForestEstimator(
        ntrees=50,
        max_depth=20,
        seed=1234
    )

    rf_reg.train(x=predictors, y=regression_target, training_frame=train)

    # Evaluar test
    perf_reg_test = rf_reg.model_performance(test_data=test)

    # Métricas detalladas
    print(f"R^2: {perf_reg_test.r2():.4f}")
    print(f"RMSE: {perf_reg_test.rmse():.4f}")
    print(f"MAE: {perf_reg_test.mae():.4f}")
    
    # Llamamos a las visualizaciones
    plot_model_results(rf_reg, perf_reg_test)
    
    return rf_reg

def gradientBoost_h2o(train, test, predictors, regression_target):
    gbm = H2OGradientBoostingEstimator(
        ntrees=200,
        max_depth=5,
        learn_rate=0.1,
        seed=1234
    )

    gbm.train(x=predictors, y=regression_target, training_frame=train)
    perf = gbm.model_performance(test_data=test)

    print("R²:", perf.r2())
    print("MSE:", perf.mse())
    print("RMSE:", perf.rmse())
    
    # Llamamos a las visualizaciones
    plot_model_results(gbm, perf)
    
    return gbm