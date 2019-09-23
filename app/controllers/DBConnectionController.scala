package controllers

import javax.inject._

import controllers.ControllerImplicits._
import entities._
import org.apache.spark.sql.DataFrame
import services.DBConnectionService
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._
import play.api.libs.json.{Json, _}
import services.SparkConfig.spark

import scala.concurrent.Future


class DBConnectionController @Inject()( dBConnectionService: DBConnectionService) extends Controller {


  def index = Action { implicit request =>
    Ok(Json.obj("status" -> "Hello World"))
  }


  //}
  def previewData =  //secured.withUser(user_id) {
    Action.async { implicit request =>
      val file_locations = "C:/Users/rk.arvind/Downloads/export.csv"

      dBConnectionService.previewData(file_locations) map { result =>
      Ok(Json.toJson(result))
      }
    }
  //}

  def filterData = Action.async(parse.json) {
      implicit request =>
        val connection = Json.fromJson[PostEntity](request.body)
        val file_location = "C:/Users/rk.arvind/Downloads/export.csv"
        connection match {
          case JsSuccess(connection: PostEntity, _) => {
            dBConnectionService.filterData(file_location, connection) map { result =>
              Ok(Json.toJson(result))
            }
          }
          case e: JsError => handleValidationError(e)
        }
    }

  private def handleValidationError(e: JsError): Future[Result] =
    Future.successful(
      Status(400)(
        Json.obj("errors" -> JsError.toJson(e).toString()))

    )


}
