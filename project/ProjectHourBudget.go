package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	project "github.com/leapforce-libraries/go_exactonline_new/project"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type ProjectHourBudget struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Budget                     float64
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	Division                   int64
	Item                       string
	ItemCode                   string
	ItemDescription            string
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Project                    string
	ProjectCode                string
	ProjectDescription         string
}

func getProjectHourBudget(p *project.ProjectHourBudget, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) ProjectHourBudget {
	t := time.Now()

	return ProjectHourBudget{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		p.ID.String(),
		p.Budget,
		go_bigquery.DateToNullTimestamp(p.Created),
		p.Creator.String(),
		p.CreatorFullName,
		p.Division,
		p.Item.String(),
		p.ItemCode,
		p.ItemDescription,
		go_bigquery.DateToNullTimestamp(p.Modified),
		p.Modifier.String(),
		p.ModifierFullName,
		p.Project.String(),
		p.ProjectCode,
		p.ProjectDescription,
	}
}

func (service *Service) WriteProjectHourBudgets(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.ProjectService().NewGetProjectHourBudgetsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		projectHourBudgets, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if projectHourBudgets == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *projectHourBudgets {
			batchRowCount++

			b, err := json.Marshal(getProjectHourBudget(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#ProjectHourBudgets flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#ProjectHourBudgets: %v\n", rowCount)

	return objectHandles, rowCount, ProjectHourBudget{}, nil
}
